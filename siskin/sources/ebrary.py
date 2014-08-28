# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301,R0904

"""
Ebrary.

Configuration keys:

[ebrary]

ftp-host = host.name
ftp-username = username
ftp-password = password
ftp-path = /path/to/file.mrc
"""

from gluish.benchmark import timed
from gluish.common import FTPFile
from gluish.esindex import CopyToIndex
from gluish.parameter import ClosestDateParameter
from gluish.path import copyregions
from siskin.task import DefaultTask
from gluish.utils import shellout
from siskin.configuration import Config
import datetime
import luigi
import tempfile

config = Config.instance()

class EbraryTask(DefaultTask):
    TAG = '024'

    def closest(self):
        return datetime.date(2010, 1, 1)

class EbraryCombine(EbraryTask):
    """ FTP mirror and combine relevant files. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return FTPFile(host=config.get('ebrary', 'ftp-host'),
            username=config.get('ebrary', 'ftp-username'),
            password=config.get('ebrary', 'ftp-password'),
            filepath=config.get('ebrary', 'ftp-path'))

    @timed
    def run(self):
        combined = shellout(r"unzip -p {input} \*.mrc > {output}",
                            input=self.input().path)

        # there is a broken record inside!
        mmap = shellout("""marcmap {input} | LANG=C grep -v ^ebr10661760 \
                           | awk '{{print $2":"$3}}' > {output} """,
                           input=combined)
        # prepare seekmap
        seekmap = []
        with open(mmap) as handle:
            for line in handle:
                offset, length = map(int, line.strip().split(':'))
                seekmap.append((offset, length))

        # create the filtered file
        _, tmp = tempfile.mkstemp(prefix='gluish-')
        with open(tmp, 'w') as output:
            with open(combined) as handle:
                copyregions(handle, output, seekmap)

        output = shellout("""yaz-marcdump -f marc8s -t utf8 -o marc
                          -l 9=97 {input} > {output}""", input=tmp)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class EbraryJson(EbraryTask):
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return EbraryCombine(date=self.date)

    @timed
    def run(self):
        tmp = shellout("marctojson -m date={date} {input} > {output}",
                       input=self.input().path, date=self.date)
        luigi.File(tmp).move(self.output().fn)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class EbraryIndex(EbraryTask, CopyToIndex):
    date = ClosestDateParameter(default=datetime.date.today())

    index = 'ebrary'
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
        return EbraryJson(date=self.date)
