# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301
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
Bibliographie des Musikschrifttums online (BMS online).

http://www.musikbibliographie.de

Configuration keys:

[bms]

ftp-host = host.name
ftp-username = username
ftp-password = password
ftp-filepath = /path/to/file

"""

from gluish.intervals import yearly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from luigi.contrib.esindex import CopyToIndex
from siskin.benchmark import timed
from siskin.common import FTPFile
from siskin.configuration import Config
from siskin.task import DefaultTask
import datetime
import luigi

config = Config.instance()

class BMSTask(DefaultTask):
    """ BMS base task. """
    TAG = '011'

    def closest(self):
        return yearly(date=self.date)

class BMSImport(BMSTask):
    """ Fetch bmsall.xml from the FTP server. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        host = config.get('bms', 'ftp-host')
        username = config.get('bms', 'ftp-username')
        password = config.get('bms', 'ftp-password')
        filepath = config.get('bms', 'ftp-filepath')
        return FTPFile(host=host, username=username, password=password,
                       filepath=filepath)

    @timed
    def run(self):
        self.input().move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))

class BMSMarc(BMSTask):
    """ Convert the XML to MARC. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return BMSImport(date=self.date)

    @timed
    def run(self):
        output = shellout("yaz-marcdump -i marcxml -o marc {input} > {output}",
                          input=self.input().path, ignoremap={5: 'INVESTIGATE'})
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class BMSJson(BMSTask):
    """ Convert the combined MARC file to a single JSON file."""
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return BMSMarc(date=self.date)

    @timed
    def run(self):
        tmp = shellout("marctojson -m date={date} {input} > {output}",
                       input=self.input().path, date=self.date)
        luigi.File(tmp).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class BMSIndex(BMSTask, CopyToIndex):

    date = ClosestDateParameter(default=datetime.date.today())

    index = 'bms'
    doc_type = 'title'
    purge_existing_index = True

    mapping = {'title': {'date_detection': False,
                          '_id': {'path': 'content.001'},
                          '_all': {'enabled': True,
                                   'term_vector': 'with_positions_offsets',
                                   'store': True}}}

    def update_id(self):
        """ This id will be a unique identifier for this indexing task. """
        return self.effective_task_id()

    def requires(self):
        return BMSJson(date=self.date)
