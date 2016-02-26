# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301
#
#  Copyright 2016 by Leipzig University Library, http://ub.uni-leipzig.de
#                 by The Finc Authors, http://finc.info
#                 by Martin Czygan, <martin.czygan@uni-leipzig.de>
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
#

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
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from luigi.contrib.esindex import CopyToIndex
from siskin.benchmark import timed
from siskin.common import FTPMirror
from siskin.configuration import Config
from siskin.task import DefaultTask
from siskin.utils import iterfiles
import collections
import datetime
import json
import luigi
import os
import re

config = Config.instance()

class ElsevierJournalsTask(DefaultTask):
    """ Jstor base. """
    TAG = '085'

    def closest(self):
        return weekly(self.date)

class ElsevierJournalsPaths(ElsevierJournalsTask):
    """
    Sync.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    max_retries = luigi.IntParameter(default=10, significant=False)
    timeout = luigi.IntParameter(default=20, significant=False, description='timeout in seconds')

    def requires(self):
        return FTPMirror(host=config.get('elsevierjournals', 'ftp-host'),
                         username=config.get('elsevierjournals', 'ftp-username'),
                         password=config.get('elsevierjournals', 'ftp-password'),
                         pattern=config.get('elsevierjournals', 'ftp-pattern'),
                         max_retries=self.max_retries,
                         timeout=self.timeout)

    def run(self):
        self.input().move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class ElsevierJournalsExpand(ElsevierJournalsTask):
    """
    Expand all tar files.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return ElsevierJournalsPaths(date=self.date)

    def run(self):
        shellout("mkdir -p {dir}", dir=self.taskdir())
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                if row.path.endswith('tar'):
                    shellout("tar -xvf {tarfile} -C {dir}", tarfile=row.path, dir=self.taskdir())

        with self.output().open('w') as output:
            for dirName, subdirList, fileList in os.walk(self.taskdir()):
                print('Found directory: %s' % dirName)
                for fname in fileList:
                    output.write_tsv(os.path.join(dirName, fname))

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class ElsevierJournalsIntermediateSchema(ElsevierJournalsTask):
    """
    All in one task for now.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    tag = luigi.Parameter(default='SAXC0000000000002')

    def requires(self):
        return ElsevierJournalsExpand(date=self.date)

    def run(self):
        patterns = {
            'dataset': re.compile('(?P<base>.*)/(?P<tag>%s)/dataset.xml' % self.tag),
            'issue': re.compile('(?P<base>.*)/(?P<tag>%s)/(?P<issn>.*)/(?P<issue>.*)/issue.xml' % self.tag),
            'main': re.compile('(?P<base>.*)/(?P<tag>%s)/(?P<issn>.*)/(?P<issue>.*)/(?P<document>.*)/main.xml' % self.tag),
        }

        doctree = collections.defaultdict(list)

        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                for name, pattern in patterns.iteritems():
                    match = pattern.match(row.path)
                    if not match:
                        continue

                    gd = match.groupdict()
                    if name == "main":
                        issuepath = "%s/issue.xml" % '/'.join(row.path.split('/')[:-2])
                        doctree[issuepath].append(row.path)

        # doctree is a of the format: {"issue.xml": ["main.xml", "main.xml", ....]}
        with self.output().open('w') as output:
            for issuepath, docs in doctree.iteritems():
                with open(issuepath) as handle:
                    issue = BeautifulStoneSoup(handle.read())

                for docpath in docs:
                    with open(docpath) as fh:
                        doc = BeautifulStoneSoup(fh.read())

                    # all information for a single intermediate schema is accessible here
                    intermediate = {
                        'finc.format': 'ElectronicArticle',
                        'finc.mega_collection': 'Elsevier Journals',
                        'finc.source_id': '85',
                        "rft.genre": "article",
                        'rft.issn': [node.text for node in issue.findAll('ce:issn')],
                        'doi': doc.find('ce:doi').text,
                        'rtf.atitle': doc.find('ce:title').text,
                    }
                    if doc.find('ce:abstract'):
                        intermediate['abstract'] = doc.find('ce:abstract').getText()[8:],

                    rawauthors = doc.findAll('ce:author')
                    authors = []
                    for author in rawauthors:
                        authors.append({'rft.au': author.find('ce:surname').text + ", " + author.find('ce:given-name').text})
                    intermediate['authors'] = authors

                    rawkeywords = doc.findAll('ce:keywords')
                    keywords = []
                    for kw in rawkeywords:
                        keywords.append(kw.find('ce:text').text)
                    intermediate['x.subjects'] = keywords

                    output.write(json.dumps(intermediate))
                    output.write("\n")

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))
