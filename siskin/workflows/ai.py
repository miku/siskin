# coding: utf-8

from gluish.common import Executable
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from siskin.sources.crossref import CrossrefItems
from siskin.sources.degruyter import DegruyterXML
from siskin.sources.doaj import DOAJDump
from siskin.sources.gbi import GBIXML
from siskin.sources.jstor import JstorXML
from siskin.task import DefaultTask
import datetime
import luigi

class AITask(DefaultTask):
    TAG = 'ai'

    def closest(self):
        return weekly(self.date)

class AIIntermediateSchema(AITask):
    """ Create an intermediate schema record from all AI sources. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'crossref': CrossrefItems(date=self.date),
            'degruyter': DegruyterXML(date=self.date),
            'doaj': DOAJDump(date=self.date),
            'genios': GBIXML(date=self.date),
            'jstor': JstorXML(date=self.date),
            '_': Executable(name='span', message='http://git.io/vI8NV'),
        }

    def run(self):
        for name, target in self.input().iteritems():
            print(name, target)

    def output(self):
        return luigi.LocalTarget(path=self.path())