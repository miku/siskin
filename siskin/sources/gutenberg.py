# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103

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
Gutenberg metadata.
"""

from luigi.contrib.esindex import CopyToIndex
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
import datetime
import luigi

class GutenbergTask(DefaultTask):
    TAG = '001'

    def closest(self):
        return weekly(date=self.date)

class GutenbergDownload(GutenbergTask):
    """ Download and extract on the fly. """
    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://www.gutenberg.org/feeds/catalog.marc.bz2", significant=False)

    def run(self):
        output = shellout("curl -L {url} | bunzip2 -c > {output}", url=self.url)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class GutenbergJson(GutenbergTask):
    """ Convert Marc to Json. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GutenbergDownload(date=self.date)

    def run(self):
        output = shellout("marctojson -m date={date} {input} > {output}",
                          date=self.closest(), input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class GutenbergIndex(GutenbergTask, CopyToIndex):
    date = ClosestDateParameter(default=datetime.date.today())

    index = 'gutenberg'
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
        return GutenbergJson(date=self.date)
