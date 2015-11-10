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
Yago.
"""

from siskin.benchmark import timed
from gluish.intervals import yearly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
import datetime
import luigi

class YagoTask(DefaultTask):
    TAG = 'yago'

    def closest(self):
        return yearly(self.date)

class YagoDump(YagoTask):
    """ Download and extract Yago dump. Might 404 from time to time. """
    date = ClosestDateParameter(default=datetime.date.today())

    @timed
    def run(self):
        url = "http://www.mpi-inf.mpg.de/yago-naga/yago/download/yago/yago2s_ttl.7z"
        output = shellout("""wget --retry-connrefused -O {output} {url}""", url=url)
        output = shellout("""7z e -so {input} 2> /dev/null > {output}""", input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ttl'))
