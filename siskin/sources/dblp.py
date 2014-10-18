# coding: utf-8

from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
import datetime
import luigi

class DBLPTask(DefaultTask):
    TAG = 'dblp'

    def closest(self):
        return monthly(date=self.date)

class DBLPDownload(DBLPTask):
    """ Download file and extract. """
    url = luigi.Parameter(default='http://dblp.uni-trier.de/xml/dblp.xml.gz', significant=False)
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("curl -L {url} | gunzip -c > {output}", url=self.url)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))
