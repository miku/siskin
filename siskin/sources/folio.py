# coding: utf-8
# pylint: disable=C0301,E1101,C0330,C0111

# Copyright 2023 by Leipzig University Library, http://ub.uni-leipzig.de
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
FOLIO filterconfig adapter

----

Config:


[folio]
okapi_url = https://some-tenant.folio.finc.info
okapi_token = XXX
"""

from gluish.parameter import ClosestDateParameter
from siskin.task import DefaultTask
from gluish.utils import shellout
from gluish.intervals import weekly
import luigi


class FolioTask(DefaultTask):
    """
    Base class for FOLIO related tasks
    """

    TAG = "folio"

    def closest(self):
        return weekly(self.date)


class FolioFilterConfigFreeze(FolioTask):
    """
    Create filterconfig for span tag from FOLIO API.
    """

    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout(
            """
            OKAPI_TOKEN={okapi_token} span-freeze -b -f -no-proxy -tenant de15 -okapi-url {okapi_url} -o {output}
            """,
            okapi_token=self.config.get("folio", "okapi_token"),
            okapi_url=self.config.get("folio", "okapi_url"),
        )
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="zip"))
