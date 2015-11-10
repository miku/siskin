# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301,R0904
#
#  Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                 by The Finc Authors, http://finc.info
#                 by Martin Czygan, <martin.czygan@uni-leipzig.de>
#
# This file is part of some open source application.
#
# Some open source application is free software: you can redistribute
# it and/or modify it under the terms of the GNU General Public
# License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.
#
# Some open source application is distributed in the hope that it will
# be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
#
# @license GPL-3.0+ <http://spdx.org/licenses/GPL-3.0+>
#

"""
Ebrary.

Configuration keys:

[ebrary]

ftp-host = host.name
ftp-username = username
ftp-password = password
ftp-path = /path/to/file.mrc
"""

from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from luigi.contrib.esindex import CopyToIndex
from siskin.benchmark import timed
from siskin.common import FTPFile
from siskin.configuration import Config
from siskin.task import DefaultTask
from siskin.utils import copyregions
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
