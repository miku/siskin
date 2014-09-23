# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

"""
Elsevier.

Configuration keys:

[elsevier]

scp-src = [user@server:[port]]/path/to/file.bin
"""

from gluish.benchmark import timed
from gluish.common import Executable
from gluish.esindex import CopyToIndex
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.configuration import Config
from siskin.task import DefaultTask
import datetime
import luigi

config = Config.instance()

class ElsevierTask(DefaultTask):
    TAG = '016'

    def closest(self):
        return datetime.date(2014, 1, 1)

class ElsevierJson(ElsevierTask):
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return [Executable(name='marcuniq'), Executable(name='marctojson')]

    @timed
    def run(self):
        output = shellout("scp {origin} {output}", origin=config.get('elsevier', 'scp-src'))
        output = shellout("marcuniq {input} > {output}", input=output)
        output = shellout("marctojson -m date={date} {input} > {output}",
                          date=self.closest(), input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class ElsevierIndex(ElsevierTask, CopyToIndex):
    date = ClosestDateParameter(default=datetime.date.today())

    index = 'elsevier'
    doc_type = 'title'
    purge_existing_index = True

    mapping = {'title': {'date_detection': False,
                          '_id': {'path': 'content.001'},
                          '_all': {'enabled': True,
                                   'term_vector': 'with_positions_offsets',
                                   'store': True}}}

    def update_id(self):
        """ This id will be a unique identifier for this indexing task."""
        return self.effective_task_id()

    def requires(self):
        return ElsevierJson(date=self.date)
