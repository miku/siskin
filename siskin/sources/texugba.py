#! coding: utf-8

from gluish.format import TSV
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from siskin.task import DefaultTask
from siskin.common import FTPMirror
import datetime
import luigi

class TexUGBATask(DefaultTask):
    TAG = 'texugba'

    def closest(self):
        return monthly(self.date)

class TexUGBASync(TexUGBATask):
    """ Mirror FTP. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return FTPMirror(host='ftp://ftp.math.utah.edu/pub/tex/bib/')

    def run(self):
        self.input().move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
