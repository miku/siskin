# coding: utf-8
# pylint: disable=C0301,E1101

# Copyright 2019 by Leipzig University Library, http://ub.uni-leipzig.de
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
NL.

Source: 17
Ticket: 15865

Config

[nl]

solr = https://example.com/vaqrk/FYHO
"""

import datetime

import luigi
from gluish.common import Executable
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask


class NLTask(DefaultTask):
    """
    Base task.
    """
    TAG = '17'

    def closest(self):
        return monthly(date=self.date)


class NLFetch(NLTask):
    """
    Stream from SOLR.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    query = luigi.Parameter(default="collection_details:GBV_NL_EBOOK",
                            description="to test: id:NLEB006936695 OR id:NLEB006936733")

    def run(self):
        """
        cf. https://github.com/stedolan/jq/issues/787, "Warning: replace is deprecated and will be removed in a future version."

        There's jq -j, but replace cannot run on a single 1GB line.
        """
        output = shellout("""solrdump -verbose -server {server} -q "{query}" -fl fullrecord | \
                          jq -r '.fullrecord' | \
                          replace '#29;' $(printf "\\x1D") '#30;' $(printf "\\x1E") '#31;' $(printf "\\x1F") > {output} """,
                          server=self.config.get('nl', 'solr'),
                          query=self.query)
        output = shellout("sed ':a;N;$!ba;s/\\x1d\\x0a/\\x1d/g' {input} > {output}", input=output)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="mrc", digest=True))

