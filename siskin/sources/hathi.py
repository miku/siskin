# coding: utf-8

from gluish.common import OAIHarvestChunk
from gluish.utils import shellout
from siskin.task import DefaultTask
import datetime
import luigi

class HathiTask(DefaultTask):
    TAG = '035'

class HathiSync(HathiTask):
    """ OAI Harvest """
    begin = luigi.DateParameter(datetime.date(2010, 1, 1))
    date = luigi.DateParameter(default=datetime.date.today())
    collection = luigi.Parameter(default='hathitrust')
    prefix = luigi.Parameter(default='marc21')
    url = luigi.Parameter(default='http://quod.lib.umich.edu/cgi/o/oai/oai', significant=False)

    def requires(self):
        return OAIHarvestChunk(begin=self.begin, end=self.date, url=self.url,
                               collection=self.collection, prefix=self.prefix)

    def run(self):
        self.input().move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

class HathiMarc(HathiTask):
    """ Convert to Marc """
    begin = luigi.DateParameter(datetime.date(2010, 1, 1))
    date = luigi.DateParameter(default=datetime.date.today())
    collection = luigi.Parameter(default='hathitrust')
    prefix = luigi.Parameter(default='marc21')
    url = luigi.Parameter(default='http://quod.lib.umich.edu/cgi/o/oai/oai', significant=False)

    def requires(self):
        return HathiSync(begin=self.begin, date=self.date, collection=self.collection, prefix=self.prefix, url=self.url)

    def run(self):
        output = shellout("yaz-marcdump -i marcxml -o marc {input} > {output}",
                          input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())
