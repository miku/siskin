# coding: utf-8

from gluish.benchmark import timed
from gluish.common import OAIHarvestChunk, Executable
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import date_range, shellout
from siskin.task import DefaultTask
import datetime
import luigi
import tempfile

class HistbestTask(DefaultTask):
    TAG = '036'

    def closest(self):
        return monthly(date=self.date)

class HistbestSync(HistbestTask):
    """ OAI Harvest """
    begin = luigi.DateParameter(datetime.date(2010, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())
    prefix = luigi.Parameter(default='marc21')
    url = luigi.Parameter(default='http://histbest.ub.uni-leipzig.de/oai2', significant=False)

    def requires(self):
        return OAIHarvestChunk(begin=self.begin, end=self.closest(), url=self.url,
                               collection=None, prefix=self.prefix)

    def run(self):
        self.input().move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

class HistbestHarvestChunk(OAIHarvestChunk, HistbestTask):
    """ Harvest all files in chunks. """

    begin = luigi.DateParameter(default=datetime.date.today())
    end = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://histbest.ub.uni-leipzig.de/oai2", significant=False)
    prefix = luigi.Parameter(default="mods-pure")
    collection = luigi.Parameter(default=None, significant=False)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))

class HistbestHarvest(luigi.WrapperTask, HistbestTask):
    """ Harvest Histbest. """
    begin = luigi.DateParameter(default=datetime.date(2010, 1, 1))
    end = luigi.DateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://histbest.ub.uni-leipzig.de/oai2", significant=False)
    prefix = luigi.Parameter(default="mods-pure")
    collection = luigi.Parameter(default=None, significant=False)

    def requires(self):
        """ Only require up to the last full month. """
        begin = datetime.date(self.begin.year, self.begin.month, 1)
        end = datetime.date(self.end.year, self.end.month, 1)
        if end < self.begin:
            raise RuntimeError('Invalid range: %s - %s' % (begin, end))
        dates = date_range(begin, end, 7, 'days')
        for i, _ in enumerate(dates[:-1]):
            yield HistbestHarvestChunk(begin=dates[i], end=dates[i + 1], url=self.url,
                                       prefix=self.prefix, collection=self.collection)

    def output(self):
        return self.input()

class HistbestCombine(HistbestTask):
    """ Combine the chunks for a date range into a single file.
    The filename will carry a name, that will only include year
    and month for begin and end. """
    begin = luigi.DateParameter(default=datetime.date(2010, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://histbest.ub.uni-leipzig.de/oai2", significant=False)
    prefix = luigi.Parameter(default="mods-pure")
    collection = luigi.Parameter(default=None, significant=False)

    def requires(self):
        return {'files': HistbestHarvest(begin=self.begin, end=self.date, prefix=self.prefix,
                                         url=self.url, collection=self.collection),
                'apps': [Executable(name='xsltproc'),
                         Executable(name='yaz-marcdump'),
                         Executable(name='marcuniq')]}

    @timed
    def run(self):
        xsl = self.assets("MODS2MARC21slim.xsl")
        _, combined = tempfile.mkstemp(prefix='siskin-')
        for target in self.input().get('files'):
            output = shellout("xsltproc {stylesheet} {input} > {output}", stylesheet=xsl, input=target.path)
            output = shellout("yaz-marcdump -f utf-8 -t utf-8 -i marcxml -o marc {input} > {output}", input=output)
            shellout("cat {input} >> {output}", input=output, output=combined)

        # output = shellout("marcuniq -o {output} {input}", input=combined)
        # luigi.File(output).move(self.output().path)
        luigi.File(combined).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))
