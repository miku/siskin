# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301

# Copyright 2018 by Leipzig University Library, http://ub.uni-leipzig.de
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

"""
Geoscan, refs #9871.

* http://geoscan.nrcan.gc.ca/rss/newpub_e.rss

XXX: Only recent works?
"""

import datetime

import luigi

from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask


class GeoscanTask(DefaultTask):
    """
    Base task for Geoscan.
    """
    TAG = "129"

    def closest(self):
        return weekly(date=self.date)


class GeoscanMARC(GeoscanTask):
    """
    Geoscan RSS to MARC. Currently, only the current feed is converted.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    link = luigi.Parameter(default="http://geoscan.nrcan.gc.ca/rss/newpub_e.rss",
                           significant=False)

    def run(self):
        output = shellout("""python {script} <(curl --fail -sL {link}) {output}""",
                          link=self.link,
                          script=self.assets("129/129_marcbinary.py"))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="fincmarc.mrc"))
