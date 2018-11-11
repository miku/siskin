# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301
# Copyright 2017 by Leipzig University Library, http://ub.uni-leipzig.de
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

"""
JOVE refs #11308, #14062.

Example input file in ticket #14062, attachment 6494.

    $ ls ~/.redminesync/14062/6494/
    'JoVE_MARC_journal 2.mrk'

Config
------

[jove]

input = path/to/some.mrk

"""

import datetime

import luigi
import requests

from gluish.utils import shellout
from siskin.task import DefaultTask


class JoveTask(DefaultTask):
    TAG = "143"


class JoveMARC(JoveTask):
    """
    Turn MRK MARC (https://www.loc.gov/marc/makrbrkr.html) into MARC XML via
    experimental marctexttoxml (https://git.io/vdCUN).
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("""marctexttoxml < {input} |
                          xmllint --format - > {output}""",
                          input=self.config.get("jove", "input"))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))


class JoveFincMARC(JoveTask):
    """
    Convert JOVE to fincmarc.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return JoveMARC(date=self.date)

    def run(self):
        output = shellout("""python {script} {input} {output}""",
                          script=self.assets("143/143_marcbinary.py"),
                          input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='fincmarc.mrc'))
