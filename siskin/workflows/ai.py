# coding: utf-8

from gluish.common import Executable
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.sources.crossref import CrossrefIntermediateSchema, CrossrefUniqISSNList
from siskin.sources.degruyter import DegruyterIntermediateSchema, DegruyterISSNList
from siskin.sources.doaj import DOAJIntermediateSchema, DOAJISSNList
from siskin.sources.gbi import GBIIntermediateSchema, GBIISSNList
from siskin.sources.holdings import HoldingsFile
from siskin.sources.jstor import JstorIntermediateSchema, JstorISSNList
from siskin.task import DefaultTask
import datetime
import itertools
import luigi
import tempfile

class AITask(DefaultTask):
    TAG = 'ai'

    def closest(self):
        return weekly(self.date)

class DownloadFile(AITask):
    """ Download a file. TODO(miku): move this out here. """

    date = luigi.DateParameter(default=datetime.date.today())
    url = luigi.Parameter()

    def run(self):
        output = shellout("""curl "{url}" > {output}""", url=self.url)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True))

class AIISSNStats(AITask):
    """ Match ISSN lists. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'crossref': CrossrefUniqISSNList(date=self.date),
            'degruyter': DegruyterISSNList(date=self.date),
            'doaj': DOAJISSNList(date=self.date),
            'gbi': GBIISSNList(date=self.date),
            'jstor': JstorISSNList(date=self.date)
        }

    def run(self):
        for k1, k2 in itertools.permutations(self.input().keys(), r=2):
            print(k1, k2)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class AIIntermediateSchema(AITask):
    """ Create an intermediate schema record from all AI sources. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return [
            CrossrefIntermediateSchema(date=self.date),
            DegruyterIntermediateSchema(date=self.date),
            DOAJIntermediateSchema(date=self.date),
            GBIIntermediateSchema(date=self.date),
            JstorIntermediateSchema(date=self.date),
        ]

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))
