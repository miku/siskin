# coding: utf-8
# pylint: disable=C0301,E1101

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
ZVDD, refs #6610.
"""

import datetime

import luigi
from gluish.format import Gzip
from gluish.intervals import quarterly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.task import DefaultTask


class ZVDDTask(DefaultTask):
    """
    Base task for ZVDD, refs #6610.
    """
    TAG = '93'

    def closest(self):
        return quarterly(self.date)


class ZVDDHarvest(ZVDDTask):
    """
    Harvest ZVDD endpoint. Default oai_dc does not contain too much
    information, harvesting METS with metha yielded errors. Use oaicrawl
    (https://git.io/v5yuQ) to harvest endpoint occasionally. One complete
    download might take several days. Data set is created in a best effort
    style (errors ignored).

    Estimated number of records (Fall 2017): ~502638.

    This harvest may take a day or two.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://zvdd.de/oai2/", significant=False)
    prefix = luigi.Parameter(default="mets", significant=False)

    def run(self):
        """
        ---- 8< ----

        Usage of oaicrawl:
        -b      create best effort data set
        -e duration
                max elapsed time (default 10s)
        -f string
                format (default "oai_dc")
        -retry int
                max number of retries (default 3)
        -verbose
                more logging
        -version
                show version
        -w int
                number of parallel connections (default 16)
        """
        output = shellout("""oaicrawl -w 12 -retry 5 -e 15s -f {prefix} -b -verbose "{url}" | pigz -c > {output}""",
                          prefix=self.prefix, url=self.url)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='data.gz'), format=Gzip)


class ZVDDIntermediateSchema(ZVDDTask):
    """
    Experimental conversion to intermediate format.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return ZVDDHarvest(date=self.date)

    def run(self):
        output = shellout("unpigz -c {input} | span-import -i zvdd-mets | pigz -c > {output}",
                          input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)
