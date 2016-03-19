# coding: utf-8

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
Web of Science.
"""

from gluish.utils import shellout
from siskin.task import DefaultTask
from siskin.sources.crossref import CrossrefDOIAndISSNList
from siskin.sources.doaj import DOAJDOIList, DOAJISSNList
from gluish.format import TSV
import datetime
import luigi
import re

class WOSTask(DefaultTask):
    TAG = 'wos'

    def closest(self):
        return monthly(date=self.date)

class WOSPublicationList(WOSTask):
    """
    Publication list. PDF -> txt.
    """
    date = luigi.Parameter(default=datetime.date.today())

    def run(self):
        output = shellout("wget -O {output} http://ip-science.thomsonreuters.com/mjl/publist_sciex.pdf")
        output = shellout("pdftotext -layout {input} {output}", input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

class WOSISSNList(WOSTask):
    """
    Rough ISSN list.
    """
    date = luigi.Parameter(default=datetime.date.today())

    def requires(self):
        return WOSPublicationList(date=self.date)

    def run(self):
        issns = set()
        with self.input().open() as handle:
            for line in handle:
                for issn in re.findall(r'[0-9]{4}-[0-9]{3}[0-9X]', line):
                    issns.add(issn)

        with self.output().open('w') as output:
            for issn in sorted(issns):
                output.write_tsv(issn)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class WOSCrossrefMatch(WOSTask):
    """
    Find all ISSNs for Web of Science export.

    The input --file must contain one DOI per line:

    005410.001002/14651858.CD14005465.pub14651852
    10.1002/ange.201106065
    10.1007/BFb0061839.56
    ..
    """
    date = luigi.DateParameter(default=datetime.date.today())
    file = luigi.Parameter(description='web of science doi list', significant=False)

    def requires(self):
        return CrossrefDOIAndISSNList(date=self.date)

    def run(self):
        # wos contains all WOS DOIs, found keeps the DOIs, that could be found in Crossref
        wos, found = set(), set()

        with open(self.file) as handle:
            for line in handle:
                wos.add(line.strip().lower())

        with self.input().open() as handle:
            with self.output().open('w') as output:
                for line in handle:
                    parts = [s.strip('"') for s in line.strip().split(',')]
                    doi, issns = parts[0].lower(), parts[1:]
                    if doi in wos:
                        output.write_tsv('OK', doi, *issns)
                        found.add(doi)

                for id in wos.difference(found):
                    output.write_tsv('NOT_FOUND', id)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class WOSMatchDOAJAndReferenceDOI(WOSTask):
    """
    For all references in WOS export, check if DOI is in DOAJ.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    file = luigi.Parameter(description='web of science doi list (references)', significant=False)

    def requires(self):
        return DOAJDOIList(date=self.date)

    def run(self):
        reference_dois = set()
        with open(self.file) as handle:
            for line in handle:
                reference_dois.add(line.strip())

        with self.input().open() as handle:
            with self.output().open('w') as output:
                for row in handle:
                    parts = row.split('\t')
                    if len(parts) < 0:
                        continue
                    doi = parts[0].strip()
                    if doi in reference_dois:
                        output.write_tsv('OPEN_ACCESS', doi)
                    else:
                        output.write_tsv('NOT_IN_DOAJ', doi)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class WOSISSNForDOI(WOSTask):
    """
    Extract ISSN for a list of DOIs.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    file = luigi.Parameter(description='web of science doi list (references)', significant=False)

    def requires(self):
        return CrossrefDOIAndISSNList(date=self.date)

    def run(self):
        reference_dois = set()
        with open(self.file) as handle:
            for line in handle:
                reference_dois.add(line.strip())

        with self.input().open() as handle:
            with self.output().open('w') as output:
                for i, line in enumerate(handle):
                    if i % 1000000 == 0:
                        self.logger.info('checked %s dois' % i)
                    line = line.replace('","', '\t').replace('"', '').strip()
                    parts = line.split('\t')
                    if len(parts) < 2:
                        continue
                    doi, issns = parts[0], parts[1:]
                    if doi in reference_dois:
                        output.write_tsv(doi, *issns)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class WOSISSNInDOAJ(WOSTask):
    """
    For all references in WOS export, check if DOI is in DOAJ.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    file = luigi.Parameter(description='web of science doi list (references)', significant=False)

    def requires(self):
        return {
            'wos-doi-list': WOSISSNForDOI(file=self.file, date=self.date),
            'doaj-issns': DOAJISSNList(date=self.date),
        }

    def run(self):
        doaj_issns = set()
        with self.input().get('doaj-issns').open() as handle:
            for line in handle:
                doaj_issns.add(line.strip())

        with self.input().get('wos-doi-list').open() as handle:
            with self.output().open('w') as output:
                for line in handle:
                    fields = line.strip().split('\t')
                    doi = fields[0]
                    for issn in fields[1:]:
                        if issn in doaj_issns:
                            output.write_tsv('IN_DOAJ', doi, issn)
                        else:
                            output.write_tsv('NOT_IN_DOAJ', doi, issn)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
