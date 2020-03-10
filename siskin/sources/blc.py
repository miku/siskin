# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301
#
# Copyright 2020 by Leipzig University Library, http://ub.uni-leipzig.de
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

Source: British Library Catalogue
SID: 181
Ticket: #15835
Origin: Z39.50
Updates: monthly

Configuration:

[blc]
z39auth = authentication USERNAME/PASSWORD

"""

import datetime

import luigi
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask


class BLCTask(DefaultTask):
    """
    Base task for BLC.
    """
    TAG = "181"

    def closest(self):
        return monthly(date=self.date)


class BLCHarvest(BLCTask):
    """
    Harvest with Z39.50 client. https://www.bl.uk/help/get-marc-21-data
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        auth = self.config.get("blc", "z39auth")
        commands = self.assets("181/commands_fine")
        output = shellout("""yaz-client -f <(cat <(echo {auth}) {commands}) -m {output} """, auth=auth, commands=commands)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="mrc"))


class BLCMARC(BLCTask):
    """
    Custom convert to FincMARC.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return BLCHarvest(date=self.date)

    def run(self):
        output = shellout("{python} {script} {input} {output}",
                          python=self.config.get("core", "python"),
                          script=self.assets("181/181_marcbinary.py"),
                          input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="fincmarc.mrc"))
