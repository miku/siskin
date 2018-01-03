# coding: utf-8
# pylint: disable=E1101,C0111
#
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
Heidelberger historische Bestände digital, #5964.
"""

import datetime

import luigi
from gluish.format import Gzip
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.task import DefaultTask


class HHBDTask(DefaultTask):
    """ Base task for source. """
    TAG = '107'

    def closest(self):
        return monthly(self.date)


class HHBDCombine(HHBDTask):
    """
    OAI, single file version.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='oai_dc')
    url = luigi.Parameter(default='http://digi.ub.uni-heidelberg.de/cgi-bin/digioai.cgi',
                          significant=False)

    def run(self):
        shellout(
            "metha-sync -format {format} http://digi.ub.uni-heidelberg.de/cgi-bin/digioai.cgi", format=self.format)
        output = shellout(
            "metha-cat -format {format} -root Records {url} > {output}", format=self.format, url=self.url)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))


class HHBDIntermediateSchema(HHBDTask):
    """
    Convert to intermediate schema via metafacture.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return HHBDCombine(date=self.date, format='oai_dc')

    def run(self):
        output = shellout(
            "span-import -i hhbd < {input} | pigz -c > {output}", input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)


class HHBDExport(HHBDTask):
    """
    Export and hard-wire label, DE-540.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='solr5vu3')

    def requires(self):
        return HHBDIntermediateSchema(date=self.date)

    def run(self):
        """ Tag by hand since not yet in AMSL. """
        output = shellout("""
	    unpigz -c {input} | span-tag -c '{{"DE-540": {{"any": {{}}}}}}' |
            span-export -with-fullrecord -o {format} > {output}
        """, format=self.format, input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))
