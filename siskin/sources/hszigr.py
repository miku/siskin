# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101

"""
HS ZI/GR

Configuration keys:

scp-src = [user@server:[port]]/path/to/file/{tag}/*mrc
"""

from gluish.benchmark import timed
from gluish.esindex import CopyToIndex
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.configuration import Config
from siskin.task import DefaultTask
import datetime
import luigi

config = Config.instance()

class HSZIGRTask(DefaultTask):
    TAG = '023'

    def closest(self):
        """ One time thing. """
        return datetime.date(2014, 1, 1)

class HSZIGRImport(HSZIGRTask):
    date = ClosestDateParameter(default=datetime.date.today())

    @timed
    def run(self):
        output = shellout("scp {origin} {output}", origin=config.get('hszigr', 'scp-src'))
        output = shellout("marcuniq -x 'BW, ZI' {input} > {output}", input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class HSZIGRJson(HSZIGRTask):
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return HSZIGRImport(date=self.date)

    @timed
    def run(self):
        output = shellout("marctojson -m date={date} {input} > {output}",
                          input=self.input().path, date=self.closest())
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class HSZIGRIndex(HSZIGRTask, CopyToIndex):
    date = ClosestDateParameter(default=datetime.date.today())

    index = 'hszigr'
    doc_type = 'title'
    pruge_existing_index = True

    mapping = {'title': {'date_detection': False,
                      '_id': {'path': 'content.001'},
                      '_all': {'enabled': True,
                               'term_vector': 'with_positions_offsets',
                               'store': True}}}
    def update_id(self):
        return self.effective_task_id()

    def requires(self):
        return HSZIGRJson(date=self.date)
