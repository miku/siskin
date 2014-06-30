# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103

"""
Gutenberg metadata.
"""

from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
import datetime
import luigi

class GutenbergTask(DefaultTask):
    TAG = '001'

    def closest(self):
        return weekly(date=self.date)

class GutenbergDownload(GutenbergTask):
    """ Download and extract on the fly. """
    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://www.gutenberg.org/feeds/catalog.marc.bz2", significant=False)

    def run(self):
        output = shellout("curl -L {url} | bunzip2 -c > {output}", url=self.url)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))
