# coding: utf-8

"""
Web of Science.
"""

from gluish.utils import shellout
from siskin.task import DefaultTask
from siskin.sources.crossref import CrossrefDOIAndISSNList
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
