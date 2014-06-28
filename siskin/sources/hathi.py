# coding: utf-8

from siskin.task import DefaultTask
from gluish.common import OAIHarvestChunk
import datetime
import luigi

class HathiTask(DefaultTask):
    TAG = '035'

class HathiSync(HathiTask):

    begin = luigi.DateParameter(datetime.date(2010, 1, 1))
    date = luigi.DateParameter(default=datetime.date.today())
    collection = luigi.Parameter(default='hathitrust')
    prefix = luigi.Parameter(default='marc21')
    url = luigi.Parameter(default='http://quod.lib.umich.edu/cgi/o/oai/oai')

    def requires(self):
        return OAIHarvestChunk(begin=self.begin, end=self.date, url=self.url,
                               collection=self.collection, prefix=self.prefix)

    def run(self):
        self.input().move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())
