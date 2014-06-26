# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

"""
Yago.
"""

from gluish.benchmark import timed
from gluish.intervals import yearly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
import datetime
import luigi

class YagoTask(DefaultTask):
    TAG = 'yago'

    def closest(self):
        return yearly(self.date)

class YagoDump(YagoTask):
    """ Download and extract Yago dump. Might 404 from time to time. """
    date = ClosestDateParameter(default=datetime.date.today())

    @timed
    def run(self):
        url = "http://www.mpi-inf.mpg.de/yago-naga/yago/download/yago/yago2s_ttl.7z"
        output = shellout("""wget --retry-connrefused -O {output} {url}""", url=url)
        output = shellout("""7z e -so {input} 2> /dev/null > {output}""", input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ttl'))
