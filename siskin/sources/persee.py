# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103

from gluish.benchmark import timed
from gluish.common import OAIHarvestChunk
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout, date_range
from siskin.task import DefaultTask
import datetime
import luigi
import tempfile

class PerseeTask(DefaultTask):
    TAG = '039'

    def closest(self):
        return monthly(date=self.date)

class PerseeHarvestChunk(OAIHarvestChunk, PerseeTask):
    """ Harvest all files in chunks. """

    begin = luigi.DateParameter(default=datetime.date.today())
    end = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://oai.persee.fr/c/ext/prescript/oai", significant=False)
    prefix = luigi.Parameter(default="marc", significant=False)
    collection = luigi.Parameter(default=None)
    delay = luigi.IntParameter(default=10, significant=False)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))

class PerseeHarvest(luigi.WrapperTask, PerseeTask):
    """ Harvest Histbest. """
    begin = luigi.DateParameter(default=datetime.date(2010, 1, 1))
    end = luigi.DateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://oai.persee.fr/c/ext/prescript/oai", significant=False)
    prefix = luigi.Parameter(default="marc", significant=False)
    collection = luigi.Parameter(default=None)
    delay = luigi.IntParameter(default=10, significant=False)

    def requires(self):
        """ Only require up to the last full month. """
        begin = datetime.date(self.begin.year, self.begin.month, 1)
        end = datetime.date(self.end.year, self.end.month, 1)
        if end < self.begin:
            raise RuntimeError('Invalid range: %s - %s' % (begin, end))
        dates = date_range(begin, end, 7, 'days')
        for i, _ in enumerate(dates[:-1]):
            yield PerseeHarvestChunk(begin=dates[i], end=dates[i + 1], url=self.url,
                                    prefix=self.prefix, collection=self.collection, delay=self.delay)

    def output(self):
        return self.input()

class PerseeCombine(PerseeTask):
    """ Combine the chunks for a date range into a single file.
    The filename will carry a name, that will only include year
    and month for begin and end. """
    begin = luigi.DateParameter(default=datetime.date(2010, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://oai.persee.fr/c/ext/prescript/oai", significant=False)
    prefix = luigi.Parameter(default="marc", significant=False)
    collection = luigi.Parameter(default=None)
    delay = luigi.IntParameter(default=10, significant=False)

    def requires(self):
        """ monthly updates """
        return PerseeHarvest(begin=self.begin, end=self.date, prefix=self.prefix,
                            url=self.url, collection=self.collection, delay=self.delay)

    @timed
    def run(self):
        _, combined = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            tmp = shellout("""yaz-marcdump -f utf-8 -t utf-8 -i
                              marcxml -o marc {input} > {output}""",
                              input=target.fn, ignoremap={5: 'TODO: fix this'})
            shellout("cat {input} >> {output}", input=tmp, output=combined)
        luigi.File(combined).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))
