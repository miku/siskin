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
GBV related tasks.

Configutation keys:

[gbv]

scp-src = [user@server:[port]]/path/to/file/{tag}/*mrc

Requirements:

    * marcuniq, marctojson
    * elasticsearch
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

class GBVTask(DefaultTask):
    """ GBV base task. """
    TAG = '021'

    def closest(self):
        """ Singular event. """
        return datetime.date(2014, 1, 1)

class GBVImport(GBVTask):
    """ Copy and merge files from a single tag on the fly. Tag is the name of
    the subdirectory, like haab, hfm or sbb.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    tag = luigi.Parameter(default='haab')

    @timed
    def run(self):
        stopover = tempfile.mkdtemp(prefix='siskin-')
        origin = config.get('gbv', 'scp-src').format(tag=self.tag)
        shellout("scp {origin} {output}", origin=origin, output=stopover)

        _, combined = tempfile.mkstemp(prefix='siskin-')
        for path in iterfiles(stopover):
            shellout("cat {input} >> {output}", input=path, output=combined)
        luigi.File(combined).move(self.output().fn)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class GBVCombine(GBVTask):
    """ Combine different files from different tags together. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        for tag in ('haab', 'hfm', 'sbb'):
            yield GBVImport(date=self.date, tag=tag)

    @timed
    def run(self):
        _, combined = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path,
                     output=combined)
        output = shellout("marcuniq {input} > {output}", input=combined)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class GBVJson(GBVTask):
    """ Convert to Json. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GBVCombine(date=self.date)

    @timed
    def run(self):
        output = shellout("marctojson -m date={date} {input} > {output}",
                          input=self.input().path, date=self.closest())
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class GBVIndex(GBVTask, CopyToIndex):
    """ Upload to ES. """
    date = ClosestDateParameter(default=datetime.date.today())

    index = 'gbv'
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
        return GBVJson(date=self.date)
