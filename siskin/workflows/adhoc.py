# coding: utf-8
# pylint: disable=C0301

# Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
#                   Martin Czygan, <martin.czygan@uni-leipzig.de>
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
# Adhoc
# =====
#

import datetime
import json
import requests
import tempfile

import luigi

from gluish.format import TSV, Gzip
from gluish.utils import shellout
from siskin.sources.amsl import AMSLService, AMSLCollections
from siskin.sources.crossref import CrossrefExport, CrossrefCollections, CrossrefCollectionsDifference, CrossrefCollectionsCount
from siskin.sources.degruyter import DegruyterExport
from siskin.sources.doaj import DOAJExport
from siskin.sources.elsevierjournals import ElsevierJournalsExport
from siskin.sources.highwire import HighwireExport
from siskin.sources.jstor import JstorExport
from siskin.task import DefaultTask
from siskin.utils import load_set_from_target, SetEncoder

import xlsxwriter


class AdhocTask(DefaultTask):
    """
    Base task for throwaway tasks.
    """
    TAG = "adhoc"


class AdhocFormetaSamples(AdhocTask):
    """
    Formeta test samples.
    """

    def requires(self):
        return [
            CrossrefExport(format='formeta'),
            DegruyterExport(format='formeta'),
            DOAJExport(format='formeta'),
            ElsevierJournalsExport(format='formeta'),
            HighwireExport(format='formeta'),
            JstorExport(format='formeta'),
        ]

    def run(self):
        with self.output().open('w') as output:
            for target in self.input():
                output.write_tsv(target.path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class OADOIDatasetStatusByDOI(AdhocTask):
    """
    Download oadoi.org dataset.
    See release notes at: https://docs.google.com/document/d/1Whfe26oyjTedeW1GGWkq3NADgDbL2R2eXqrJCWS8vcc/edit#

    > These datasets are for non-commercial use. For commercial use, ask us
      about our Support Level Agreement (team@impactstory.org). For more
      information about oaDOI, see http://oadoi.org.
    """

    def run(self):
        url = "https://s3-us-west-2.amazonaws.com/oadoi-datasets/oa_status_by_doi.csv.gz"
        output = shellout("""wget -O "{output}" "{url}" """, url=url)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="csv.gz"), format=Gzip)


class Issue7049(AdhocTask):
    """
    Prepare a few bits about collections, refs #7049.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'amsl': AMSLCollections(date=self.date),
            'crossref': CrossrefCollections(date=self.date),
        }

    def run(self):
        amsl = load_set_from_target(self.input().get('amsl'))
        crossref = load_set_from_target(self.input().get('crossref'))

        with self.output().open('w') as output:
            stats = {
                'amsl': amsl,
                'crossref': crossref,
                'amsl_only': amsl - crossref,
                'crossref_only': crossref - amsl,
                'both': amsl & crossref,
            }
            output.write(json.dumps(stats, cls=SetEncoder))

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="json"))


class Issue7049ExportExcel(AdhocTask):
    """
    Create a simple Excel file, with one sheet per kind.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'issue7049': Issue7049(date=self.date),
            'counts': CrossrefCollectionsCount(date=self.date),
        }

    def run(self):
        with self.input().get('counts').open() as handle:
            counts = json.load(handle)

        with self.input().get('issue7049').open() as handle:
            doc = json.load(handle)

        _, stopover = tempfile.mkstemp(prefix='siskin-')
        workbook = xlsxwriter.Workbook(stopover)

        worksheet = workbook.add_worksheet(name='Overview')

        keys = doc.keys()

        worksheet.write(0, 0, "#7049")
        worksheet.write(
            1, 0, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        for i, key in enumerate(keys, start=3):
            worksheet.write(i, 1, key)
            worksheet.write(i, 2, len(doc[key]))

        for _, key in enumerate(keys):
            worksheet = workbook.add_worksheet(name=key)
            for j, item in enumerate(sorted(doc[key])):
                worksheet.write(j, 0, counts.get(item, 'NA'))
                worksheet.write(j, 1, item)

        workbook.close()
        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="xlsx"))


class K10Matches(AdhocTask):
    """
    How many of the ISSN in K10Plus are indexed in finc?

    CSV list: https://is.gd/AW3bCB
    """

    ai = luigi.Parameter(default="http://localhost:8983/solr/biblio")
    finc = luigi.Parameter(default="http://localhost:8983/solr/biblio")

    def run(self):
        r = requests.get("https://is.gd/AW3bCB")
        with self.output().open("w") as output:
            for line in r.text.split('\n'):
                try:
                    issn, count = line.split(',')

                    results = {}

                    results['ai'] = requests.get("%s/select?q=issn:%s&rows=0&wt=json" %
                                                 (self.ai, issn))
                    if results['ai'].status_code != 200:
                        raise RuntimeError(
                            "ai reponded with %s" % rr.status_code)

                    results['finc'] = requests.get("%s/select?q=issn:%s&rows=0&wt=json" %
                                                   (self.finc, issn))
                    if results['finc'].status_code != 200:
                        raise RuntimeError(
                            "finc reponded with %s" % rr.status_code)

                    output.write_tsv(issn, count,
                                     str(results['ai'].json().get(
                                         "response").get("numFound")),
                                     str(results['finc'].json().get(
                                         "response").get("numFound")),
                                     )

                except Exception as exc:
                    self.logger.debug(exc)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True), format=TSV)
