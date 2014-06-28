# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

"""
Vifa.

Configuration keys:

[vifa]

scp-src = [user@server:[port]]/path/to/file.mrc
"""

from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.configuration import Config
from siskin.task import DefaultTask
import datetime
import luigi

config = Config.instance()

class VifaTask(DefaultTask):
    """ Generic Vifa task. """
    TAG = '029'

    def closest(self):
        return datetime.date(2014, 1, 1)

class VifaSync(VifaTask):
    """ Copy over file. """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("scp {src} {output}", src=config.get('vifa', 'scp-src'))
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

class VifaJson(VifaTask):
    """ Convert to Json. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return VifaSync(date=self.date)

    def run(self):
        output = shellout("marctojson -m date={date} {input} > {output}",
                          input=self.input().path, date=self.closest())
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())
