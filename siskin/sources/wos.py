# coding: utf-8

"""
Web of Science.
"""

from gluish.utils import shellout
from siskin.task import DefaultTask
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
    Publication list.
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
