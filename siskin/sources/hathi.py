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

class HathiTask(DefaultTask):
    TAG = '035'

    def closest(self):
        return monthly(date=self.date)

class HathiHarvestChunk(OAIHarvestChunk, HathiTask):
    """ Harvest all files in chunks. """

    begin = luigi.DateParameter(default=datetime.date.today())
    end = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://quod.lib.umich.edu/cgi/o/oai/oai", significant=False)
    prefix = luigi.Parameter(default="marc21", significant=False)
    collection = luigi.Parameter(default='hathitrust')
    delay = luigi.IntParameter(default=10, significant=False)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))

class HathiHarvest(luigi.WrapperTask, HathiTask):
    """ Harvest Histbest. """
    begin = luigi.DateParameter(default=datetime.date(2010, 1, 1))
    end = luigi.DateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://quod.lib.umich.edu/cgi/o/oai/oai", significant=False)
    prefix = luigi.Parameter(default="marc21", significant=False)
    collection = luigi.Parameter(default='hathitrust')
    delay = luigi.IntParameter(default=10, significant=False)

    def requires(self):
        """ Only require up to the last full month. """
        begin = datetime.date(self.begin.year, self.begin.month, 1)
        end = datetime.date(self.end.year, self.end.month, 1)
        if end < self.begin:
            raise RuntimeError('Invalid range: %s - %s' % (begin, end))
        dates = date_range(begin, end, 7, 'days')
        for i, _ in enumerate(dates[:-1]):
            yield HathiHarvestChunk(begin=dates[i], end=dates[i + 1], url=self.url,
                                    prefix=self.prefix, collection=self.collection, delay=self.delay)

    def output(self):
        return self.input()

class HathiCombine(HathiTask):
    """ Combine the chunks for a date range into a single file.
    The filename will carry a name, that will only include year
    and month for begin and end. """
    begin = luigi.DateParameter(default=datetime.date(2010, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://quod.lib.umich.edu/cgi/o/oai/oai", significant=False)
    prefix = luigi.Parameter(default="marc21", significant=False)
    collection = luigi.Parameter(default='hathitrust')
    delay = luigi.IntParameter(default=10, significant=False)

    def requires(self):
        """ monthly updates """
        return HathiHarvest(begin=self.begin, end=self.date, prefix=self.prefix,
                            url=self.url, collection=self.collection, delay=self.delay)

    @timed
    def run(self):
        _, combined = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            tmp = shellout("""yaz-marcdump -f utf-8 -t utf-8 -i
                              marcxml -o marc {input} > {output}""",
                              input=target.fn, ignoremap={5: 'TODO: fix this'})
            shellout("cat {input} >> {output}", input=tmp, output=combined)
        output = shellout("marcuniq -o {output} {input}", input=combined)
        luigi.File(output).move(self.output().fn)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))
