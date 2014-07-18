# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

from gluish.common import OAIHarvestChunk
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from siskin.task import DefaultTask
from gluish.utils import shellout
import datetime
import luigi


class TUFGoobiTask(DefaultTask):
    TAG = 'tufgoobi'

    def closest(self):
        return monthly(date=self.date)

class TUFGoobiCombine(TUFGoobiTask):
    """ Digital library via Goobi (#3207) """
    begin = luigi.DateParameter(default=datetime.date(2000, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())
    prefix = luigi.Parameter(default="oai_dc", significant=False)
    url = luigi.Parameter(default="http://digital.ub.tu-freiberg.de/oai", significant=False)
    collection = luigi.Parameter(default=None, significant=False)

    def requires(self):
        return OAIHarvestChunk(begin=self.begin, end=self.date,
                               prefix=self.prefix, url=self.url,
                               collection=self.collection)

    def run(self):
        output = shellout("xsltproc {stylesheet} {input} > {output}",
                          input=self.input().path,
                          stylesheet=self.assets('OAIDCtoMARCXML.xsl'))
        output = shellout("yaz-marcdump -i marcxml -o marc {input} > {output}",
                          input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))
