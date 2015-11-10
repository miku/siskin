# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301
#
#  Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
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
VIAF-related tasks.
"""

from siskin.benchmark import timed
from luigi.contrib.esindex import CopyToIndex
from gluish.format import TSV
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
import BeautifulSoup
import datetime
import luigi
import re
import requests

class VIAFTask(DefaultTask):
    TAG = 'viaf'

    def closest(self):
        task = VIAFLatestDate()
        luigi.build([task], local_scheduler=True)
        with task.output().open() as handle:
            date, _ = handle.iter_tsv(cols=('date', 'url')).next()
            dateobj = datetime.date(*map(int, date.split('-')))
        return dateobj

class VIAFDataURLList(VIAFTask):
    """ Download Viaf data page.
        Match on http://viaf.org/viaf/data/viaf-20140215-links.txt.gz
    """
    date = luigi.DateParameter(default=datetime.date.today())
    url = luigi.Parameter(default='http://viaf.org/viaf/data/')

    def run(self):
        r = requests.get(self.url)
        if not r.status_code == 200:
            raise RuntimeError(r)

        strainer = BeautifulSoup.SoupStrainer('a')
        soup = BeautifulSoup.BeautifulSoup(r.text, parseOnlyThese=strainer)
        with self.output().open('w') as output:
            for link in soup:
                output.write_tsv(link.get('href'))

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True), format=TSV)

class VIAFLatestDate(VIAFTask):
    """" The latest date for clusters file """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return VIAFDataURLList()

    def run(self):
        with self.input().open() as handle:
            for row in handle:
                pattern = r"http://viaf.org/viaf/data/viaf-(\d{8})-clusters-rdf.nt.gz"
                mo = re.match(pattern, row)
                if mo:
                    date = datetime.datetime.strptime(mo.group(1), '%Y%m%d').date()
                    with self.output().open('w') as output:
                        output.write_tsv(date, mo.group())
                    break
            else:
                raise RuntimeError('Could not find latest date.')

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class VIAFDump(VIAFTask):
    """ Download the latest dump. Dynamic, uses the link from VIAFLatestDate. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return VIAFLatestDate()

    def run(self):
        with self.input().open() as handle:
            _, url = handle.iter_tsv(cols=('date', 'url')).next()
            output = shellout("wget -c --retry-connrefused '{url}' -O {output}",
                              url=url)
            luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='nt.gz'))

class VIAFExtract(VIAFTask):
    """ Extract the dump. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return VIAFDump(date=self.date)

    def run(self):
        output = shellout("gunzip -c {input} > {output}",
                          input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='nt'))

class VIAFPredicateDistribution(VIAFTask):
    """ Just a uniq -c on the predicate 'column' """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return VIAFExtract(date=self.date)

    def run(self):
        output = shellout("""cut -d " " -f2 {input} | LANG=C sort | LANG=C uniq -c > {output}""",
                          input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='txt'))

class VIAFAbbreviatedNTriples(VIAFTask):
    """ Get a Ntriples representation of VIAF, but abbreviate with ntto. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return VIAFExtract(date=self.date)

    @timed
    def run(self):
        output = shellout("ntto -a -r {rules} -o {output} {input}", input=self.input().path, rules=self.assets('RULES.txt'))
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='nt'))

class VIAFSameAs(VIAFTask):
    """ Extract the VIAF sameAs relations. """

    date = ClosestDateParameter(default=datetime.date.today())
    predicate = luigi.Parameter(default='<http://schema.org/sameAs>', description='older dumps use <http://www.w3.org/2002/07/owl#sameAs>')

    def requires(self):
        return VIAFExtract(date=self.date)

    def run(self):
        output = shellout("""LANG=C grep -F "{predicate}" {input} > {output}""",
                          predicate=self.predicate, input=self.input().path, ignoremap={1: "Not found."})
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='nt', digest=True))

class VIAFDBPediaLinks(VIAFTask):
    """ Extract the VIAF sameAs to dbpedia. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return VIAFSameAs(date=self.date)

    def run(self):
        output = shellout("""LANG=C grep -F "http://dbpedia.org/resource" {input} > {output}""", input=self.input().path, ignoremap={1: "Not found."})
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='nt'))

class VIAFJson(VIAFTask):
    """ Shorter notation. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return VIAFExtract(date=self.date)

    @timed
    def run(self):
        output = shellout("""ntto -o {output} -a -i {input} &&
            cat {output} |
            LANG=C grep -Fv "This primary entity identifier is deprecated" |
            LANG=C grep -Fv "This concept identifier is deprecated" |
            LANG=C grep -Fv "foaf:primaryTopic" |
            LANG=C grep -Fv "void:inDataset" |
            LANG=C grep -Fv "foaf:focus" |
            LANG=C grep -Fv "foaf:Document" |
            LANG=C grep -Fv "/#foaf:Organization" |
            LANG=C grep -Fv "/#rdaEnt:CorporateBody" |
            LANG=C grep -Fv "/#foaf:Person" > {output}""",
            input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class VIAFIndex(VIAFTask, CopyToIndex):
    date = ClosestDateParameter(default=datetime.date.today())

    index = 'viaf'
    doc_type = 'triples'
    purge_existing_index = True

    def update_id(self):
        """ This id will be a unique identifier for this indexing task."""
        return self.effective_task_id()

    def requires(self):
        return VIAFJson(date=self.date)
