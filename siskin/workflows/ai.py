# coding: utf-8

from gluish.common import Executable
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.sources.crossref import CrossrefItems
from siskin.sources.degruyter import DegruyterXML
from siskin.sources.doaj import DOAJDump
from siskin.sources.gbi import GBIXML
from siskin.sources.holdings import HoldingsFile
from siskin.sources.jstor import JstorXML
from siskin.task import DefaultTask
import datetime
import luigi

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
            '_': Executable(name='span-import', message='http://git.io/vI8NV'),
        }

    def run(self):
        _, stopover = tempfile.mkdtemp(prefix='siskin-')
        for format, target in self.input().iteritems():
            if name == "_":
                continue
            shellout("span -i {format} {input} >> {output}", format=format, input=target.path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class AIImportable(AITask):
    """ Create target, that is importable into SOLR. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'input': AIIntermediateSchema(date=self.date),
            'DE-15': HoldingsFile(date=self.date, isil='DE-15'),
            'DE-14': HoldingsFile(date=self.date, isil='DE-14'),
            'DE-105': HoldingsFile(date=self.date, isil='DE-105'),
            'DE-Ch1': HoldingsFile(date=self.date, isil='DE-Ch1'),
            'DE-Gla1': HoldingsFile(date=self.date, isil='DE-Gla1'),
            'DE-Bn3': HoldingsFile(date=self.date, isil='DE-Bn3'),
            'DE-15-FID': DownloadFile(date=self.date, url='https://goo.gl/8P6JtB')
        }

    def run(self):
        for name, target in self.input():
            print(name, target)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))
