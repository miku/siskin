# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301

# Copyright 2017 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
#                   Robert Schenk, <robert.schenk@uni-leipzig.de>
#                   Martin Czygan, <martin.czygan@uni-leipzig.de>
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
B3Kat, #8697.
"""

import datetime
import tempfile

import luigi
from gluish.format import Gzip
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.task import DefaultTask


class B3KatTask(DefaultTask):
    TAG = 'b3kat'

    def closest(self):
        return datetime.date(2017, 5, 1)


class B3KatDownload(B3KatTask):
    """
    Download snapshot. Adjust the number of files with parameter 'last', which
    holds the id of the last part.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    last = luigi.IntParameter(default=30, description='number of parts', significant=False)

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for i in range(1, self.last + 1):
            link = 'https://www.bib-bvb.de/OpenData/b3kat_export_{year:04d}_{month:02d}_teil{part:02d}.xml.gz'.format(
                year=self.closest().year, month=self.closest().month, part=i)
            shellout("""curl -sL --fail "{link}" >> {stopover} """, link=link, stopover=stopover)
        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml.gz'), format=Gzip)
