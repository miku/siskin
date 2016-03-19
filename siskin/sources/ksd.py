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
KSD.

Configuration keys:

[ksd]

scp-src = [user@server:[port]]/path/to/*.mrc

"""

from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from luigi.contrib.esindex import CopyToIndex
from siskin.benchmark import timed
from siskin.configuration import Config
from siskin.task import DefaultTask
from siskin.utils import iterfiles
import datetime
import luigi
import tempfile

config = Config.instance()

class KSDTask(DefaultTask):
    TAG = '019'

    def closest(self):
        """ Single shipment. """
        return datetime.date(2014, 1, 1)

class KSDImport(KSDTask):
    """ Add the file on mdma (or wherever) and adjust the latest date. """
    date = ClosestDateParameter(default=datetime.date.today())
    tag = luigi.Parameter('haab')

    @timed
    def run(self):
        # gather files
        stopover = tempfile.mkdtemp(prefix='siskin-')
        shellout("scp {origin} {output}", origin=config.get('ksd', 'scp-src'), output=stopover)
        # combine files
        _, combined = tempfile.mkstemp(prefix='siskin-')
        for path in sorted(iterfiles(stopover), reverse=True):
            shellout("cat {input} >> {output}", input=path, output=combined)
        # clean dups
        output = shellout("marcuniq {input} > {output}", input=combined)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class KSDJson(KSDTask):
    """ Convert to JSON. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return KSDImport(date=self.date)

    @timed
    def run(self):
        output = shellout("marctojson -m date={date} {input} > {output}",
                        input=self.input().path, date=self.closest())
        luigi.File(output).move(self.output().path)

    def output(self):
        """ Use monthly files. """
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class KSDIndex(KSDTask, CopyToIndex):
    date = ClosestDateParameter(default=datetime.date.today())

    index = 'ksd'
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
        return KSDJson(date=self.date)
