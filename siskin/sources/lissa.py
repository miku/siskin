# coding: utf-8
# pylint: disable=C0301,E1101

# Copyright 2019 by Leipzig University Library, http://ub.uni-leipzig.de
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
LISSA, https://lissarchive.org/

> A free, open scholarly platform for library and information science,
https://osf.io/preprints/lissa/discover

Source: 179
Ticket: 15839
"""

import datetime

import luigi
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask


class LissaTask(DefaultTask):
    """
    Base task.
    """
    TAG = '179'

    def closest(self):
        return weekly(date=self.date)


class LissaFetch(LissaTask):
    """
    Fetch dataset via single curl request with large size to ES.

    Fixed endpoint at: https://share.osf.io/api/v2/search/creativeworks/

    In 08/2019, there are only about 230 items, so a single request window of
    1000 suffices. Adjust, if dataset grows.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        es = "https://share.osf.io/api/v2/search/creativeworks"
        query = "sources:%22LIS%20Scholarship%20Archive%22"
        output = shellout("""curl -s --fail "{es}/_search?from=0&size=1000&q={query}" > {output}""",
                         query=query, es=es)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="json"))

