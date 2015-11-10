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
PQDT Open.
"""

from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
import datetime
import luigi

class PQDTTask(DefaultTask):
    TAG = '034'

    def closest(self):
        return monthly(self.date)

class PQDTSync(PQDTTask):
    begin = luigi.DateParameter(default=datetime.date(2000, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        output = shellout("oaimi -verbose http://pqdtoai.proquest.com/OAIHandler > {output}")
        luigi.File(output).move(self.output().path)

    def run(self):
        self.input().copy(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

