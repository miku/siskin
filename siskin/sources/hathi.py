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

from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.benchmark import timed
from siskin.task import DefaultTask
import datetime
import luigi
import tempfile

class HathiTask(DefaultTask):
    TAG = '035'

    def closest(self):
        return monthly(date=self.date)


class HathiDump(HathiTask):
    """ Dump SSOAR. """

    def requires(self):
        return Executable(name='oaimi', message='https://github.com/miku/oaimi')

    def run(self):
        output = shellout("""
            cat <(echo '<collection xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">')
                <(oaimi -verbose -prefix marc21 -set hathitrust http://quod.lib.umich.edu/cgi/o/oai/oai)
                <(echo '</collection>') > {output}""")
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

class HathiCombine(HathiTask):
    """ Combine the chunks for a date range into a single file.
    The filename will carry a name, that will only include year
    and month for begin and end. """

    def requires(self):
        return SSOARDump()

    @timed
    def run(self):
        output = shellout("""yaz-marcdump -f utf-8 -t utf-8 -i marcxml -o marc {input} > {output}""", input=self.input().path, ignoremap={5: 'TODO: fix this'})
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class HathiJson(HathiTask):
    """ Json version. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return SSOARCombine(date=self.date)

    def run(self):
        output = shellout("marctojson -m date={date} {input} > {output}",
                          input=self.input().path, date=self.closest())
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))