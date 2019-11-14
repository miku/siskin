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
OLC, refs #16279.

A list of (51) collection names and internal identifiers can be found here:
https://is.gd/8DFvOo (GBV).
"""

import datetime
import os

import luigi

from gluish.format import Zstd
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask


class OLCTask(DefaultTask):
    """
    OLC base task.
    """
    TAG = '68'

    def closest(self):
        return monthly(date=self.date)


class OLCDump(OLCTask):
    """
    Fetch a slice (or all of a SOLR), refs #16279.

    Currently, 9/51 collections are requested, but that might change over time.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    collection = luigi.Parameter(default='SSG-OLC-ARC', description='SSG-OLC-XXX, see: https://is.gd/8DFvOo')

    def run(self):
        output = shellout(""" solrdump -server {server} -q 'collection_details:{collection}' | zstd -q -c > {output} """,
                          server=self.config.get('olc', 'solr'),
                          collection=self.collection)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

