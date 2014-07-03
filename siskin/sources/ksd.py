# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

"""
KSD.

Configuration keys:

[ksd]

scp-src = [user@server:[port]]/path/to/*.mrc

"""

from gluish.benchmark import timed
from gluish.esindex import CopyToIndex
from gluish.parameter import ClosestDateParameter
from gluish.path import iterfiles
from gluish.utils import shellout
from siskin.configuration import Config
from siskin.task import DefaultTask
import datetime
import luigi
import tempfile

config = Config.instance()

class KSDTask(DefaultTask):
    TAG = '019'

    def closest(self):
        """ Single shipment. """
        return datetime.date(2014, 1, 1)

class KSDImport(KSDTask):
    """ Add the file on mdma (or wherever) and adjust the latest date. """
    date = ClosestDateParameter(default=datetime.date.today())
    tag = luigi.Parameter('haab')

    @timed
    def run(self):
        # gather files
        stopover = tempfile.mkdtemp(prefix='siskin-')
        shellout("scp {origin} {output}", origin=config.get('ksd', 'scp-src'), output=stopover)
        # combine files
        _, combined = tempfile.mkstemp(prefix='siskin-')
        for path in sorted(iterfiles(stopover), reverse=True):
            shellout("cat {input} >> {output}", input=path, output=combined)
        # clean dups
        output = shellout("marcuniq {input} > {output}", input=combined)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class KSDJson(KSDTask):
    """ Convert to JSON. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return KSDImport(date=self.date)

    @timed
    def run(self):
        output = shellout("marctojson -m date={date} {input} > {output}",
                        input=self.input().path, date=self.closest())
        luigi.File(output).move(self.output().path)

    def output(self):
        """ Use monthly files. """
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class KSDIndex(KSDTask, CopyToIndex):
    date = ClosestDateParameter(default=datetime.date.today())

    index = 'ksd'
    doc_type = 'title'

    mapping = {'title': {'date_detection': False,
                      '_id': {'path': 'content.001'},
                      '_all': {'enabled': True,
                               'term_vector': 'with_positions_offsets',
                               'store': True}}}

    def update_id(self):
        """ This id will be a unique identifier for this indexing task."""
        return self.effective_task_id()

    def requires(self):
        return KSDJson(date=self.date)
