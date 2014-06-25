# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

"""
IMSLP.

Configuration keys:

[imslp]

scp-src = [user@server:[port]]/path/to/file.txt
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

class IMSLPTask(DefaultTask):
    TAG = '015'

    def closest(self):
        return datetime.date(2014, 1, 1)

class IMSLPJson(IMSLPTask):
    date = ClosestDateParameter(default=datetime.date.today())

    @timed
    def run(self):
        output = shellout("scp {src} {output}", src=config.get('imslp', 'scp-src'))
        output = shellout("php {script} {input} > {output}", input=output, script=self.assets('imslp2marcxml.php'))
        output = shellout("yaz-marcdump -i marcxml -o marc {input} > {output}", input=output)
        output = shellout("marctojson -m date={date} {input} > {output}", date=self.closest(), input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class IMSLPIndex(IMSLPTask, CopyToIndex):
    date = ClosestDateParameter(default=datetime.date.today())

    index = 'imslp'
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
        return IMSLPJson(date=self.date)
