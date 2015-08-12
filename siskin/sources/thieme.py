# coding: utf-8

from gluish.common import OAIHarvestChunk
from gluish.parameter import ClosestDateParameter
from gluish.utils import date_range, shellout
from gluish.intervals import monthly
from siskin.task import DefaultTask
import datetime
import luigi
import tempfile

class ThiemeTask(DefaultTask):
    """ Thieme connect. """
    TAG = 'thieme'

    def closest(self):
	return monthly(date=self.date)

class ThiemeHarvestChunk(OAIHarvestChunk, ThiemeTask):
    """ Harvest all files in chunks. """

    begin = luigi.DateParameter(default=datetime.date.today())
    end = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="https://www.thieme-connect.de/oai/provider", significant=False)
    prefix = luigi.Parameter(default="tm", significant=True)
    collection = luigi.Parameter(default='journalarticles')
    delay = luigi.IntParameter(default=2, significant=False)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))

class ThiemeHarvest(luigi.WrapperTask, ThiemeTask):
    """ Harvest Thieme. """
    begin = luigi.DateParameter(default=datetime.date(1970, 1, 1))
    end = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="https://www.thieme-connect.de/oai/provider", significant=False)
    prefix = luigi.Parameter(default="tm", significant=True)
    collection = luigi.Parameter(default='journalarticles')
    delay = luigi.IntParameter(default=2, significant=False)

    def requires(self):
        """ Only require up to the last full month. """
        begin = datetime.date(self.begin.year, self.begin.month, 1)
        end = datetime.date(self.end.year, self.end.month, 1)
        if end < self.begin:
            raise RuntimeError('Invalid range: %s - %s' % (begin, end))
        dates = date_range(begin, end, 1, 'months')
        for i, _ in enumerate(dates[:-1]):
            yield ThiemeHarvestChunk(begin=dates[i], end=dates[i + 1], url=self.url,
                                     prefix=self.prefix, collection=self.collection, delay=self.delay)

    def output(self):
        return self.input()

class ThiemeCombine(ThiemeTask):
    """ Combine files. Simple cat files and leave span-import the rest. """

    begin = luigi.DateParameter(default=datetime.date(1970, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="https://www.thieme-connect.de/oai/provider", significant=False)
    prefix = luigi.Parameter(default="tm")
    collection = luigi.Parameter(default='journalarticles')
    delay = luigi.IntParameter(default=2, significant=False)

    def requires(self):
        return ThiemeHarvest(begin=self.begin, end=self.closest(), url=self.url, prefix=self.prefix,
                             collection=self.collection, delay=self.delay)

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="xml"))
