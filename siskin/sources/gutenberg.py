# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103

"""
Gutenberg metadata.
"""

from gluish.esindex import CopyToIndex
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

class GutenbergJson(GutenbergTask):
    """ Convert Marc to Json. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GutenbergDownload(date=self.date)

    def run(self):
        output = shellout("marctojson -m date={date} {input} > {output}",
                          date=self.closest(), input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class GutenbergIndex(GutenbergTask, CopyToIndex):
    date = ClosestDateParameter(default=datetime.date.today())

    index = 'gutenberg'
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
        return GutenbergJson(date=self.date)
