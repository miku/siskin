# coding: utf-8

from gluish.common import Executable
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from gluish.format import TSV
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
        output = shellout("""curl --fail "{url}" > {output}""", url=self.url)
        luigi.File(output).move(self.output().path)

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
        def loadset(target):
            s = set()
            with target.open() as handle:
                for row in handle.iter_tsv(cols=('issn',)):
                    s.add(row.issn)
            return s

        with self.output().open('w') as output:
            for k1, k2 in itertools.combinations(self.input().keys(), 2):
                s1 = loadset(self.input().get(k1))
                s2 = loadset(self.input().get(k2))
                output.write_tsv(k1, k2, len(s1), len(s2), len(s1.intersection(s2)))

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class AIISSNOverlaps(AITask):
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
        def loadset(target):
            s = set()
            with target.open() as handle:
                for row in handle.iter_tsv(cols=('issn',)):
                    s.add(row.issn)
            return s

        with self.output().open('w') as output:
            for k1, k2 in itertools.combinations(self.input().keys(), 2):
                s1 = loadset(self.input().get(k1))
                s2 = loadset(self.input().get(k2))
                for issn in sorted(s1.intersection(s2)):
                    output.write_tsv(k1, k2, issn)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class AIExport(AITask):
    """ Create a SOLR-importable file. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'crossref': CrossrefIntermediateSchema(date=self.date),
            'degruyter': DegruyterIntermediateSchema(date=self.date),
            'doaj': DOAJIntermediateSchema(date=self.date),
            'gbi': GBIIntermediateSchema(date=self.date),
            'jstor' : JstorIntermediateSchema(date=self.date),

            'DE-105': HoldingsFile(isil='DE-105', date=self.closest()),
            'DE-14': HoldingsFile(isil='DE-14', date=self.closest()),
            'DE-15': HoldingsFile(isil='DE-15', date=self.closest()),
            'DE-Bn3': HoldingsFile(isil='DE-Bn3', date=self.closest()),
            'DE-Ch1': HoldingsFile(isil='DE-Ch1', date=self.closest()),
            'DE-Gla1': HoldingsFile(isil='DE-Gla1', date=self.closest()),
            # 'DE-Zi4': HoldingsFile(isil='DE-Zi4', date=self.closest()),
            # 'DE-J59': HoldingsFile(isil='DE-J59', date=self.closest()),

            'DE-15-FID': DownloadFile(date=self.date, url='https://goo.gl/8P6JtB'),

            'app': Executable(name='span-export', message='http://git.io/vI8NV'),
        }

    def run(self):
        """ TODO(miku): filter DOAJ ISSNs """
        _, stopover = tempfile.mkstemp(prefix='siskin-')

        shellout("span-export -any DE-15 {input} >> {output}", input=self.input().get('gbi').path, output=stopover)
        shellout("span-export -any DE-15 {input} >> {output}", input=self.input().get('doaj').path, output=stopover)

        hkeys = ('DE-105', 'DE-14', 'DE-15', 'DE-Bn3', 'DE-Ch1', 'DE-Gla1', 'DE-Zi4')
        files = " ".join(["-f %s:%s" % (k, v.path) for k, v in self.input() if k in hkeys])

        fkeys = ('DE-15-FID',)
        lists = " ".join(["-l %s:%s" % (k, v.path) for k, v in self.input() if k in fkeys])

        for source in ('crossref', 'jstor', 'degruyter'):
            shellout("span-export {files} {lists} {input} >> {output}", files=files,
                     lists=lists, input=self.input().get(source).path, output=stopover)

        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))
