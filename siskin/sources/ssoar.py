# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301

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

"""
SSOAR workflow with OAI harvest and metafacture.
"""

import datetime

import luigi
from gluish.format import Gzip
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.decorator import deprecated
from siskin.task import DefaultTask


class SSOARTask(DefaultTask):
    """ Base task for SSOAR """
    TAG = '30'

    def closest(self):
        return monthly(date=self.date)


class SSOARHarvest(SSOARTask):
    """
    Harvest from default http://www.ssoar.info/OAIHandler/request OAI endpoint.
    """
    format = luigi.Parameter(default="marcxml")
    date = ClosestDateParameter(default=datetime.date.today())
    endpoint = luigi.Parameter(default='http://www.ssoar.info/OAIHandler/request', significant=False)

    def run(self):
        shellout("""metha-sync -format {format} {endpoint} """, endpoint=self.endpoint, format=self.format)
        output = shellout("""metha-cat -format {format} -root Records {endpoint} > {output}""",
                          endpoint=self.endpoint, format=self.format)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))


class SSOARMARC(SSOARTask):
    """
    Use RS script for conversion, refs #12686.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='oai_dc')

    def requires(self):
        return SSOARHarvest(date=self.date, format=self.format)

    @deprecated
    def run(self):
        output = shellout("""python {script} {input} {output}""",
                          script=self.assets('30/30_marcbinary.py'), input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='fincmarc.mrc'))


class SSOARIntermediateSchema(SSOARTask):
    """
    Use span-import for conversion.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return SSOARHarvest(date=self.date, format='marcxml')

    def run(self):
        output = shellout("""span-import -i ssoar {input} | pigz -c > {output}""",
                          input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)


class SSOARExport(SSOARTask):
    """
    Export to finc solr schema by using span-export.
    """
    format = luigi.Parameter(default='solr5vu3', description='export format')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return SSOARIntermediateSchema(date=self.date)

    def run(self):
        output = shellout("""unpigz -c {input} | span-export -with-fullrecord -o {format} | pigz -c > {output}""",
                          format=self.format, input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='fincsolr.ndj.gz'), format=Gzip)
