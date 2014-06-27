# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

"""
SSOAR.
"""

from gluish.common import OAIHarvestChunk
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
import datetime
import luigi

class SSOARTask(DefaultTask):
    TAG = '030'

    def closest(self):
        return monthly(self.date)

class SSOARSync(SSOARTask):
    """ Harvest around 28000 records in one go. Might take a while. """
    begin = luigi.DateParameter(default=datetime.date(2000, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())
    prefix = luigi.Parameter(default='oai_dc')

    def requires(self):
        return OAIHarvestChunk(begin=self.begin, end=self.date, prefix=self.prefix,
                               url='http://www.ssoar.info/OAIHandler/request')

    def run(self):
        self.input().copy(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))

class SSOARConvert(SSOARTask):
    """ Use the same procedure as in Disson/013. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return SSOARSync(date=self.date)

    def run(self):
        xsl = self.assets("013_OAIDCtoMARCXML.xsl")
        output = shellout("xsltproc {stylesheet} {input} > {output}",
                          stylesheet=xsl, input=self.input().path)
        output = shellout("yaz-marcdump -i marcxml -o marc {input} > {output}", input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class SSOARJson(SSOARTask):
    """ Json version. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return SSOARConvert(date=self.date)

    def run(self):
        output = shellout("marctojson -m date={date} {input} > {output}",
                          input=self.input().path, date=self.closest())
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))
