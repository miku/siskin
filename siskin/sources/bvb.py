# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

"""
BVB via OAI.
"""

from gluish.benchmark import timed
from gluish.common import OAIHarvestChunk
from gluish.esindex import CopyToIndex
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import date_range, shellout
from siskin.task import DefaultTask
import datetime
import luigi
import tempfile

class BVBTask(DefaultTask):
    TAG = '012'

    def closest(self):
        return monthly(date=self.date)

class BVBHarvestChunk(OAIHarvestChunk, BVBTask):
    """ Harvest all files in chunks. """

    begin = luigi.DateParameter(default=datetime.date.today())
    end = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://bvbr.bib-bvb.de:8991/aleph-cgi/oai/oai_opendata.pl", significant=False)
    prefix = luigi.Parameter(default="marc21", significant=False)
    collection = luigi.Parameter(default="OpenData", significant=False)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))

class BVBHarvest(luigi.WrapperTask, BVBTask):
    """ Harvest BVB, cut all dates to the first of month. Example:
    2013-05-12 .. 2013-09-11 would be translated to
    2013-05-01 .. 2013-09-01

    Note: About 10G in total?
    """
    begin = luigi.DateParameter(default=datetime.date(2011, 1, 1))
    end = luigi.DateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://bvbr.bib-bvb.de:8991/aleph-cgi/oai/oai_opendata.pl", significant=False)
    prefix = luigi.Parameter(default="marc21", significant=False)
    collection = luigi.Parameter(default="OpenData", significant=False)

    def requires(self):
        """ Only require up to the last full month. """
        begin = datetime.date(self.begin.year, self.begin.month, 1)
        end = datetime.date(self.end.year, self.end.month, 1)
        if end < self.begin:
            raise RuntimeError('Invalid range: %s - %s' % (begin, end))
        dates = date_range(begin, end, 7, 'days')
        for i, _ in enumerate(dates[:-1]):
            yield BVBHarvestChunk(begin=dates[i], end=dates[i + 1], url=self.url,
                                  prefix=self.prefix, collection=self.collection)

    def output(self):
        return self.input()

class BVBCombine(BVBTask):
    """ Combine the chunks for a date range into a single file.
    The filename will carry a name, that will only include year
    and month for begin and end. """
    begin = luigi.DateParameter(default=datetime.date(2011, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://bvbr.bib-bvb.de:8991/aleph-cgi/oai/oai_opendata.pl", significant=False)
    prefix = luigi.Parameter(default="marc21", significant=False)
    collection = luigi.Parameter(default="OpenData", significant=False)

    def requires(self):
        """ monthly updates """
        return BVBHarvest(begin=self.begin, end=self.date, prefix=self.prefix,
                          url=self.url, collection=self.collection)

    @timed
    def run(self):
        _, combined = tempfile.mkstemp(prefix='tasktree-')
        for target in self.input():
            tmp = shellout("""yaz-marcdump -f utf-8 -t utf-8 -i
                              marcxml -o marc {input} > {output}""",
                              input=target.fn, ignoremap={5: 'TODO: fix this'})
            shellout("cat {input} >> {output}", input=tmp, output=combined)
        output = shellout("marcuniq -o {output} {input}", input=combined)
        luigi.File(output).move(self.output().fn)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class BVBJson(BVBTask):
    """ Convert to JSON. """

    begin = luigi.DateParameter(default=datetime.date(2011, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return BVBCombine(begin=self.begin, date=self.date)

    def run(self):
        tmp = shellout("marctojson -m date={date} {input} > {output}",
                       input=self.input().fn, date=self.date)
        luigi.File(tmp).move(self.output().fn)

    def output(self):
        """ Use monthly files. """
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class BVBIndex(BVBTask, CopyToIndex):
    begin = luigi.DateParameter(default=datetime.date(2011, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())

    index = "bvb"
    doc_type = "title"
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
        return BVBJson(begin=self.begin, date=self.date)
