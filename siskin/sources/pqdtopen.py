# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101

"""
PQDT Open.
"""

from gluish.common import OAIHarvestChunk
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from siskin.task import DefaultTask
import datetime
import luigi

class PQDTTask(DefaultTask):
    TAG = '034'

    def closest(self):
        return monthly(self.date)

class PQDTSync(PQDTTask):
    begin = luigi.DateParameter(default=datetime.date(2000, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return OAIHarvestChunk(begin=self.begin, end=self.date,
                               url='http://pqdtoai.proquest.com/OAIHandler',
                               prefix='oai_dc')

    def run(self):
        self.input().copy(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

