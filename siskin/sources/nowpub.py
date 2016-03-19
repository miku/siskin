# coding: utf-8

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

from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
import datetime
import luigi

class NowpubTask(DefaultTask):
    TAG = "nowpub"

    def closest(self):
        return monthly(date=self.date)

class NowpubMarc(NowpubTask):
    """ Download zipfile an combine marc records. """

    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("wget -O {output} http://www.nowpublishers.com/Public-Content/marc-records/fntall.zip")
        output = shellout("unzip -p {input} *mrc > {output}", input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class NowpubJson(NowpubTask):
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return NowpubMarc(date=self.date)

    def run(self):
        output = shellout("marctojson {input} > {output}", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))
