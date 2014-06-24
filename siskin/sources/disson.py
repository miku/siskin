# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

from gluish.benchmark import timed
from gluish.common import OAIHarvestChunk, Executable
from gluish.esindex import CopyToIndex
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import date_range, shellout
from siskin.task import DefaultTask
import datetime
import luigi
import tempfile

class DissonTask(DefaultTask):
    TAG = '013'

    def closest(self):
        return monthly(date=self.date)

class DissonHarvest(luigi.WrapperTask, DissonTask):
    """ Harvest Disson. """

    begin = luigi.DateParameter(default=datetime.date(2000, 1, 1))
    end = ClosestDateParameter(default=datetime.date.today())
    prefix = luigi.Parameter(default="oai_dc", significant=False)
    url = luigi.Parameter(default="http://services.dnb.de/oai/repository", significant=False)
    collection = luigi.Parameter(default="dnb:online:dissertations", significant=False)

    def requires(self):
        """ Harvest in 1-month chunks. """
        if self.end < self.begin:
            raise RuntimeError('Invalid range: %s - %s' % (self.begin, self.end))
        dates = date_range(self.begin, self.end, 1, 'months')
        for begin, end in zip(dates[:-1], dates[1:]):
            yield OAIHarvestChunk(begin=begin, end=end, prefix=self.prefix,
                                  url=self.url, collection=self.collection)

    def output(self):
        return self.input()

class DissonCombine(DissonTask):
    """ Combine the chunks for a date range into a single file.
    The filename will carry a name, that will only include year
    and month for begin and end, example:

    .../013/disson-combine/2010-01--2013-11.mrc"""

    begin = luigi.DateParameter(default=datetime.date(2000, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())
    prefix = luigi.Parameter(default="oai_dc", significant=False)
    url = luigi.Parameter(default="http://services.dnb.de/oai/repository", significant=False)
    collection = luigi.Parameter(default="dnb:online:dissertations", significant=False)

    def requires(self):
        return {'files': DissonHarvest(begin=self.begin, end=self.date,
                                       prefix=self.prefix, url=self.url,
                                       collection=self.collection),
                'apps': [Executable(name='xsltproc'),
                         Executable(name='yaz-marcdump'),
                         Executable(name='marcuniq')]}

    @timed
    def run(self):
        xsl = self.assets("013_OAIDCtoMARCXML.xsl")
        _, combined = tempfile.mkstemp(prefix='siskin-')
        for target in self.input().get('files'):
            output = shellout("xsltproc {stylesheet} {input} > {output}",
                              stylesheet=xsl, input=target.path)
            output = shellout("yaz-marcdump -i marcxml -o marc {input} > {output}",
                              input=output)
            shellout("cat {input} >> {output}", input=output, output=combined)

        output = shellout("marcuniq -o {output} {input}", input=combined)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class DissonJson(DissonTask):
    """ Convert to JSON. """

    begin = luigi.DateParameter(default=datetime.date(2000, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return DissonCombine(begin=self.begin, date=self.date)

    def run(self):
        tmp = shellout("marctojson -m date={date} {input} > {output}",
                       input=self.input().path, date=self.date)
        luigi.File(tmp).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class DissonIndex(DissonTask, CopyToIndex):
    begin = luigi.DateParameter(default=datetime.date(2000, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())

    index = "disson"
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
        return DissonJson(begin=self.begin, date=self.date)
