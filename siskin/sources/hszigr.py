# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101
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
HS ZI/GR

Configuration keys:

scp-src = [user@server:[port]]/path/to/file/{tag}/*mrc
"""

from siskin.benchmark import timed
from luigi.contrib.esindex import CopyToIndex
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.configuration import Config
from siskin.task import DefaultTask
import datetime
import luigi

config = Config.instance()

class HSZIGRTask(DefaultTask):
    TAG = '023'

    def closest(self):
        """ One time thing. """
        return datetime.date(2014, 1, 1)

class HSZIGRImport(HSZIGRTask):
    date = ClosestDateParameter(default=datetime.date.today())

    @timed
    def run(self):
        output = shellout("scp {origin} {output}", origin=config.get('hszigr', 'scp-src'))
        output = shellout("marcuniq -x 'BW, ZI' {input} > {output}", input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class HSZIGRJson(HSZIGRTask):
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return HSZIGRImport(date=self.date)

    @timed
    def run(self):
        output = shellout("marctojson -m date={date} {input} > {output}",
                          input=self.input().path, date=self.closest())
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class HSZIGRIndex(HSZIGRTask, CopyToIndex):
    date = ClosestDateParameter(default=datetime.date.today())

    index = 'hszigr'
    doc_type = 'title'
    pruge_existing_index = True

    mapping = {'title': {'date_detection': False,
                      '_id': {'path': 'content.001'},
                      '_all': {'enabled': True,
                               'term_vector': 'with_positions_offsets',
                               'store': True}}}
    def update_id(self):
        return self.effective_task_id()

    def requires(self):
        return HSZIGRJson(date=self.date)
