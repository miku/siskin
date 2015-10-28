# coding: utf-8

from gluish.common import Executable
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
import datetime
import luigi

class ThiemeTask(DefaultTask):
    """ Thieme connect. """
    TAG = 'thieme'

    def closest(self):
        return monthly(date=self.date)

class ThiemeCombine(ThiemeTask):
    """ Combine files."""

    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="https://www.thieme-connect.de/oai/provider", significant=False)
    prefix = luigi.Parameter(default="tm")
    collection = luigi.Parameter(default='journalarticles')

    def requires(self):
        return Executable(name='oaimi', message='https://github.com/miku/oaimi')

    def run(self):
        output = shellout("oaimi -root records -prefix {prefix} -set {collection} -verbose {url} > {output}",
                          prefix=self.prefix, collection=self.collection, url=self.url)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="xml"))
