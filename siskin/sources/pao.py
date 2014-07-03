# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101

"""
PA Online

Configuration keys:

[pao]

scp-src = [user@server:[port]]/path/to/file/*mrc
deletions = [user@server:[port]]/path/to/paodel.mrc

"""

from gluish.benchmark import timed
from gluish.common import Executable
from gluish.esindex import CopyToIndex
from gluish.parameter import ClosestDateParameter
from gluish.path import iterfiles
from gluish.utils import shellout
from siskin.configuration import Config
from siskin.task import DefaultTask
import datetime
import luigi
import re
import tempfile

config = Config.instance()

class PAOTask(DefaultTask):
    TAG = '007'

    def closest(self):
        """ One time data source. """
        return datetime.date(2014, 1, 1)

class PAOSync(PAOTask):
    date = ClosestDateParameter(default=datetime.date.today())

    @timed
    def run(self):
        stopover = tempfile.mkdtemp(prefix='siskin-')
        shellout("scp {origin} {stopover}".format(origin=config.get('pao', 'scp-src'), stopover=stopover))
        _, combined = tempfile.mkstemp(prefix='siskin-')
        for path in iterfiles(directory=stopover,
                              fun=lambda path: re.search(r'pao[\d].mrc', path)):
            shellout("cat {path} >> {output}", path=path, output=combined)
        luigi.File(combined).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class PAODeletions(PAOTask):
    date = ClosestDateParameter(default=datetime.date.today())

    @timed
    def run(self):
        output = shellout("scp {deletions} {output}", deletions=config.get('pao', 'deletions'))
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class PAOJson(PAOTask):
    """ Convert to JSON, respect deletions. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'additions': PAOSync(date=self.date),
                'deletions': PAODeletions(date=self.date),
                'apps': [Executable(name='marctotsv'),
                         Executable(name='marcuniq')]
        }

    @timed
    def run(self):
        # sanitize
        additions = shellout("""yaz-marcdump -f iso-8859-1 -t utf-8
                             -i marc -o marc {input} > {output}""",
                             input=self.input().get('additions').path)
        deletions = shellout("""yaz-marcdump -f iso-8859-1 -t utf-8
                             -i marc -o marc {input} > {output}""",
                             input=self.input().get('deletions').path)

        # filter deletions
        excludes = shellout("""marctotsv {input} 001 > {output} """,
                            input=deletions)
        output = shellout("""marcuniq -x {excludes} -o {output} {input} """,
                       excludes=excludes, input=additions)

        # to json
        output = shellout("marctojson -m date={date} {input} > {output}",
                          date=self.closest(), input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class PAOIndex(PAOTask, CopyToIndex):
    date = ClosestDateParameter(default=datetime.date.today())

    index = 'pao'
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
        return PAOJson(date=self.date)
