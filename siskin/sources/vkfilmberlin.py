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
VKFilmBerlin, UdK Berlin, #8697.

TODO: Unify VK* tasks.
"""

import datetime

import luigi
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.sources.b3kat import B3KatDownload
from siskin.task import DefaultTask


class VKFilmBerlinTask(DefaultTask):
    TAG = '117'

    def closest(self):
        return monthly(date=self.date)


class VKFilmBerlinMARC(VKFilmBerlinTask):
    """
    Next version of filter and transformation, refs #12169. Takes about 4h.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return B3KatDownload(date=self.date)

    def run(self):
        """
        XXX: Nichtsortierzeichen?
        """
        output = shellout(
            "marccount {input} > {output}", input=self.input().path)
        record_count = int(open(output).read().strip())
        output = shellout("python {script} {input} {output} {record_count}",
                          script=self.assets("117/117_marcbinary.py"),
                          input=self.input().path,
                          record_count=record_count)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='fincmarc.mrc'))
