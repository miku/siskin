# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

#  Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                    The Finc Authors, http://finc.info
#                    Martin Czygan, <martin.czygan@uni-leipzig.de>
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

"""
Elsevier.

Configuration keys:

[elsevier]

scp-src = [user@server:[port]]/path/to/file.bin
"""

from gluish.common import Executable
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from luigi.contrib.esindex import CopyToIndex
from siskin.benchmark import timed
from siskin.configuration import Config
from siskin.task import DefaultTask
import datetime
import luigi

config = Config.instance()

class ElsevierTask(DefaultTask):
    TAG = '016'

    def closest(self):
        return datetime.date(2014, 1, 1)

class ElsevierJson(ElsevierTask):
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return [Executable(name='marcuniq'), Executable(name='marctojson')]

    @timed
    def run(self):
        output = shellout("scp {origin} {output}", origin=config.get('elsevier', 'scp-src'))
        output = shellout("marcuniq {input} > {output}", input=output)
        output = shellout("marctojson -m date={date} {input} > {output}",
                          date=self.closest(), input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class ElsevierIndex(ElsevierTask, CopyToIndex):
    date = ClosestDateParameter(default=datetime.date.today())

    index = 'elsevier'
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
        return ElsevierJson(date=self.date)
