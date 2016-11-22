# coding: utf-8
# pylint: disable=C0301

# Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
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

import datetime

import luigi

from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask


class DBLPTask(DefaultTask):
    TAG = 'dblp'

    def closest(self):
        return monthly(date=self.date)

class DBLPDownload(DBLPTask):
    """ Download file and extract. """
    url = luigi.Parameter(default='http://dblp.uni-trier.de/xml/dblp.xml.gz', significant=False)
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("curl --fail -L {url} > {output}", url=self.url)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml.gz'))

class DBLPDOIList(DBLPTask):
    """ QnD doi list. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return DBLPDownload()

    def run(self):
        output = shellout("""LC_ALL=C grep "doi.org" <(unpigz -c {input}) | LC_ALL=C sed -e 's@<ee>http://dx.doi.org/@@g' |
                             LC_ALL=C sed -e 's@</ee>@@g' | LC_ALL=C sort -S50% > {output}""", input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())
