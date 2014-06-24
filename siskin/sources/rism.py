# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

from __future__ import print_function
from gluish.benchmark import timed
from gluish.common import Executable
from gluish.esindex import CopyToIndex
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
import datetime
import luigi
import urllib

class RISMTask(DefaultTask):
    TAG = '014'

    def closest(self):
        return datetime.date(2014, 1, 1)

class RISMSync(RISMTask):
    """ Get at most n records (defaults to 20000). """
    date = ClosestDateParameter(default=datetime.date.today())
    maximum = luigi.IntParameter(default=20000, significant=False)

    def requires(self):
        return Executable(name='wget')

    @timed
    def run(self):
        base = 'http://service1.rism.info/opac'
        params = {
            'operation': 'searchRetrieve',
            'version': '1.1',
            'query': 'source=http*',
            'maximumRecords': self.maximum,
        }
        output = shellout("""wget --retry-connrefused -O {output} "{base}?{params}" """,
                          base=base, params=urllib.urlencode(params))
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))

class RISMMarc(RISMTask):
    """ Convert IMSLP XML (trans, antizebra) to MARCXML, then to MARC """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'file': RISMSync(date=self.date),
                'apps': [Executable(name='xsltproc'),
                         Executable(name='yaz-marcdump'),
                         Executable(name='marcuniq')]}

    @timed
    def run(self):
        # first trans.xsl
        output = shellout("xsltproc {stylesheet} {input} > {output}",
                          input=self.input().get('file').path,
                          stylesheet=self.assets('trans.xsl'))
        # then antizebra.xsl
        output = shellout("xsltproc {stylesheet} {input} > {output}",
                          input=output, stylesheet=self.assets('antizebra.xsl'))
        # then binary marc
        output = shellout("yaz-marcdump -i marcxml -o marc {input} > {output}",
                          input=output)
        # then marcuniq
        output = shellout("marcuniq -o {output} {input}", input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class RISMJson(RISMTask):
    """ Convert MARC to JSON. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return RISMMarc(date=self.date)

    @timed
    def run(self):
        output = shellout("marctojson -m date={date} {input} > {output}",
                          input=self.input().path, date=self.closest())
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class RISMIndex(RISMTask, CopyToIndex):
    date = ClosestDateParameter(default=datetime.date.today())

    index = 'rism'
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
        return RISMJson(date=self.date)
