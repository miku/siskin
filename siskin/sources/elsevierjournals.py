# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

#  Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                    The Finc Authors, http://finc.info
#                    Martin Czygan, <martin.czygan@uni-leipzig.de>
#
# This file is part of some open source application.
#
# Some open source application is free software: you can redistribute
# it and/or modify it under the terms of the GNU General Public
# License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.
#
# Some open source application is distributed in the hope that it will
# be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
#
# @license GPL-3.0+ <http://spdx.org/licenses/GPL-3.0+>

"""
Elsevier jounrals. Refs. #6975.

Configuration keys:

[elsevierjournals]

ftp-host = sftp://host.name
ftp-username = username
ftp-password = password
ftp-path = /
ftp-pattern = *

"""

from BeautifulSoup import BeautifulStoneSoup
from gluish.common import Executable
from gluish.format import TSV
from gluish.intervals import weekly
from gluish.utils import shellout
from siskin.benchmark import timed
from siskin.common import FTPMirror
from siskin.configuration import Config
from siskin.task import DefaultTask
from siskin.utils import iterfiles
import base64
import collections
import datetime
import json
import luigi
import os
import re
import tempfile

config = Config.instance()

class ElsevierJournalsTask(DefaultTask):
    """ Jstor base. """
    TAG = '085'

class ElsevierJournalsPaths(ElsevierJournalsTask):
    """
    Sync.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    max_retries = luigi.IntParameter(default=10, significant=False)
    timeout = luigi.IntParameter(default=20, significant=False, description='timeout in seconds')

    def requires(self):
        return FTPMirror(host=config.get('elsevierjournals', 'ftp-host'),
                         username=config.get('elsevierjournals', 'ftp-username'),
                         password=config.get('elsevierjournals', 'ftp-password'),
                         pattern=config.get('elsevierjournals', 'ftp-pattern'),
                         max_retries=self.max_retries,
                         timeout=self.timeout)

    @timed
    def run(self):
        self.input().move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class ElsevierJournalsExpand(ElsevierJournalsTask):
    """
    Expand all tar files.
    """
    tag = luigi.Parameter(default='SAXC0000000000002')

    def requires(self):
        return ElsevierJournalsPaths()

    @timed
    def run(self):
        shellout("mkdir -p {dir}", dir=self.taskdir())
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                if not self.tag in row.path:
                    continue
                if row.path.endswith('tar'):
                    shellout("tar -xvf {tarfile} -C {dir}", tarfile=row.path, dir=self.taskdir())

        with self.output().open('w') as output:
            for dirName, subdirList, fileList in os.walk(self.taskdir()):
                for fname in fileList:
                    output.write_tsv(os.path.join(dirName, fname))

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class ElsevierJournalsIntermediateSchema(ElsevierJournalsTask):
    """
    All in one task for now.

    Test indexing:

        $ span-solr $(taskoutput ElsevierJournalsIntermediateSchema) > x.ldj
        $ solrbulk x.ldj

    """
    tag = luigi.Parameter(default='SAXC0000000000002')

    def requires(self):
        return ElsevierJournalsExpand(tag=self.tag)

    @timed
    def run(self):
        pattern = re.compile('(?P<base>.*)/(?P<tag>%s)/(?P<issn>.*)/(?P<issue>.*)/(?P<document>.*)/main.xml' % self.tag)

        # doctree groups main files under issue files: {"issue.xml": ["main.xml", "main.xml", ....]}
        doctree = collections.defaultdict(list)

        # the dataset.xml contains the journal titles
        datasetpath = os.path.join(os.path.dirname(self.input().path), self.tag, 'dataset.xml')
        self.logger.info('dataset: %s' % datasetpath)

        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                if not pattern.match(row.path):
                    continue

                issuepath = "%s/issue.xml" % '/'.join(row.path.split('/')[:-2])
                if os.path.exists(issuepath):
                    doctree[issuepath].append(row.path)
                else:
                    self.logger.warning('issue.xml not in expected place for %s' % row.path)

        if not doctree:
            raise RuntimeError('no documents found')

        with open(datasetpath) as handle:
            dataset = BeautifulStoneSoup(handle.read(), convertEntities=BeautifulStoneSoup.HTML_ENTITIES)

        # map ISSN to journal title
        titlemap = collections.defaultdict(str)
        for props in dataset.findAll('journal-issue-properties'):
            try:
                titlemap[props.find('issn').text] = props.find('collection-title').text
            except AttributeError as err:
                self.logger.warning('cannot find issn or collection title: %s, %s, %s' % (props.find('issn'), props.find('collection-title'), err))

        with self.output().open('w') as output:
            for issuepath, docs in doctree.iteritems():

                with open(issuepath) as handle:
                    issue = BeautifulStoneSoup(handle.read(), convertEntities=BeautifulStoneSoup.HTML_ENTITIES)

                for docpath in docs:
                    with open(docpath) as fh:
                        doc = BeautifulStoneSoup(fh.read(), convertEntities=BeautifulStoneSoup.HTML_ENTITIES)

                    intermediate = {
                        'finc.format': 'ElectronicArticle',
                        'finc.mega_collection': 'Elsevier Journals',
                        'finc.source_id': '85',
                        'rft.genre': 'article',
                        'rft.issn': [node.text for node in issue.findAll('ce:issn')],
                        'doi': doc.find('ce:doi').text,
                        'url': ['http://dx.doi.org/%s' % doc.find('ce:doi').text],
                        'languages': ['eng'],
                        'ris.type': 'EJOUR',
                        'version': '0.9',
                    }

                    if doc.find('ce:title'):
                        intermediate['rft.atitle'] = doc.find('ce:title').text

                    intermediate['rft.jtitle'] = titlemap[intermediate['rft.issn'][0]]
                    intermediate['finc.record_id'] = base64.b64encode(intermediate['url'][0]).rstrip("=")

                    if doc.find('ce:abstract'):
                        abstract = doc.find('ce:abstract').getText()
                        if abstract.startswith('Abstract'):
                            abstract = abstract.replace('Abstract', '', 1)
                        if abstract.startswith(u'Highlights•'):
                            abstract = abstract.replace(u'Highlights•', '', 1)
                        if abstract.startswith(u'SummaryBackground'):
                            abstract = abstract.replace(u'SummaryBackground', '', 1)
                        abstract = abstract.replace(u'•', ' ')
                        intermediate['abstract'] = abstract

                    authors = []
                    for author in doc.findAll('ce:author'):
                        au = {}
                        given, surname = author.find('ce:given-name'), author.find('ce:surname')
                        if given:
                            au.update({'rft.aufirst': given.text})
                        if surname:
                            au.update({'rft.aulast': surname.text})
                        if surname and given:
                            au.update({'rft.au': surname.text + ", " + given.text})
                        authors.append(au)
                    intermediate['authors'] = authors

                    keywords = []
                    for kw in doc.findAll('ce:keywords'):
                        keywords.append(kw.find('ce:text').text)
                    intermediate['x.subjects'] = keywords

                    # page numbers
                    for item in issue.findAll('ce:include-item'):
                        doi = item.find('ce:doi').text
                        if doi == intermediate['doi']:
                            first, last = item.find('ce:first-page'), item.find('ce:last-page')
                            if first:
                                intermediate['rft.spage'] = ''.join(c for c in first.text if c.isdigit())
                            if last:
                                intermediate['rft.epage'] = ''.join(c for c in last.text if c.isdigit())
                            if first and last:
                                try:
                                    intermediate['rft.pages'] = str(int(intermediate['rft.epage']) - int(intermediate['rft.spage']))
                                    intermediate['rft.tpages'] = intermediate['rft.pages']
                                except ValueError as err:
                                    self.logger.warning('cannot parse page number %s: %s-%s' % (doi, first.text, last.text))

                    # volume, issue, date
                    if issue.find('vol-first'):
                        intermediate['rft.volume'] = issue.find('vol-first').text
                    if issue.find('iss-first'):
                        intermediate['rft.issue'] = issue.find('iss-first').text
                    if issue.find('start-date'):
                        date = issue.find('start-date').text
                        if len(date) == 4:
                            intermediate['rft.date'] = "%s-01-01" % (date)
                        elif len(date) == 6:
                            intermediate['rft.date'] = "%s-%s-01" % (date[:4], date[4:6])
                        elif len(date) == 8:
                            intermediate['rft.date'] = "%s-%s-%s" % (date[:4], date[4:6], date[6:8])
                        else:
                            raise ValueError("unknown date format: %s" % date)

                    output.write(json.dumps(intermediate))
                    output.write("\n")

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class ElsevierJournalsSolr(ElsevierJournalsTask):
    """
    Create something solr importable. Attach a single ISIL to all records.
    """
    tag = luigi.Parameter(default='SAXC0000000000002')

    def requires(self):
        return ElsevierJournalsIntermediateSchema(tag=self.tag)

    @timed
    def run(self):
        output = shellout("""span-tag -c <(echo '{{"DE-15": {{"any": {{}}}}}}') {input} > {output}""", input=self.input().path)
        output = shellout("span-solr {input} > {output}", input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))


class ElsevierJournalsSolrCombined(ElsevierJournalsTask):
    """
    Combine a set of tags into a single file.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        tags = [
            "SAXC0000000000009",
            "SAXC0000000000010",
            "SAXC0000000000011",
            "SAXC0000000000012",
            "SAXC0000000000013",
            "SAXC0000000000014",
            "SAXC0000000000015",
        ]
        for tag in tags:
            yield ElsevierJournalsSolr(tag=tag)

    def run(self):
        _, output = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path, output=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))
