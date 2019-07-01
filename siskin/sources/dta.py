# coding: utf-8

# Copyright 2019 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
#                   Martin Czygan, <martin.czygan@uni-leipzig.de>
#                   Robert Schenk, <robert.schenk@uni-leipzig.de>
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
DTA, refs #14972

"""

import datetime

import luigi

from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask


class DTATask(DefaultTask):
    """ Base task for DTA """
    TAG = '44'

    def closest(self):
        return monthly(date=self.date)


class DTAHarvest(DTATask):
    """
    Harvest from default https://clarin.bbaw.de/oai-dta/?verb=ListRecords&set=dta&metadataPrefix=cmdi endpoint.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        shellout("""metha-sync -rm -format cmdi -set dta https://clarin.bbaw.de/oai-dta/""")
        output = shellout("""metha-cat -format cmdi -set dta https://clarin.bbaw.de/oai-dta/ > {output}""")
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))


class DTAMARC(DTATask):
    """
    Use RS script for conversion, refs #14972.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return DTAHarvest(date=self.date)

    def run(self):
        output = shellout("""python {script} {input} {output}""",
                          script=self.assets('44/44_marcbinary.py'),
                          input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='fincmarc.mrc'))
