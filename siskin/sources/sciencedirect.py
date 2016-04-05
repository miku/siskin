# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301

#  Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                    The Finc Authors, http://finc.info
#                    Martin Czygan, <martin.czygan@uni-leipzig.de>
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
ScienceDirect Holdings.

Configuration
-------------

[sciencedirect]

holdings = /path/to/kbart.txt
"""

from gluish.format import TSV
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.benchmark import timed
from siskin.common import FTPMirror
from siskin.configuration import Config
from siskin.task import DefaultTask
import datetime
import luigi

config = Config.instance()

class ScienceDirectTask(DefaultTask):
    """ Jstor base. """
    TAG = 'pubmed'

    def closest(self):
	return monthly(self.date)

class ScienceDirectHoldings(ScienceDirectTask):
    """
    Go to http://www.sciencedirect.com/science/ehr and download KBART.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
	output = shellout("cp {input} {output}", input=config.get('sciencedirect', 'holdings'))
	luigi.File(output).move(self.output().path)

    def output(self):
	return luigi.LocalTarget(path=self.path(), format=TSV)

class ScienceDirectIdentifierList(ScienceDirectTask):
    """
    Extract identifier list and open access note.
    """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
	return ScienceDirectHoldings(date=self.date)

    def run(self):
	output = shellout(r"csvcut -t -c 3,15 {input} > {output}", input=self.input().path)
	luigi.File(output).move(self.output().path)

    def output(self):
	return luigi.LocalTarget(path=self.path(), format=TSV)

class ScienceDirectOpenAccessIdentifierList(ScienceDirectTask):
    """
    Extract identifier list and open access note.
    """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
	return ScienceDirectIdentifierList(date=self.date)

    def run(self):
	output = shellout("""grep -i "Contains Open Access" {input} | cut -d ',' -f1 > {output}""", input=self.input().path)
	luigi.File(output).move(self.output().path)

    def output(self):
	return luigi.LocalTarget(path=self.path(), format=TSV)
