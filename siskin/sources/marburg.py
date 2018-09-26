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
Medienwissenschaft, Rezensionen, Reviews. Archiv, Marburg, refs #5486, #11005.

Previously:

* http://archiv.ub.uni-marburg.de/ep/0002/oai

Now (2018-04-23):

* http://archiv.ub.uni-marburg.de/ubfind/OAI/Server

"""

import datetime
import json
import os
import tempfile

import luigi

import xmltodict
from gluish.format import Gzip
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.sources.amsl import AMSLFilterConfig
from siskin.task import DefaultTask


class MarburgTask(DefaultTask):
    """
    Base task for Marburg.
    """
    TAG = '73'

    def closest(self):
        return weekly(self.date)


class MarburgCombine(MarburgTask):
    """
    Harvest and combine a given set into a single file.

    NLM format has been discontinued as of 2018-01-01, refs #5486. Using datacite.
    """

    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='datacite')
    set = luigi.Parameter(default='issn:2196-4270')

    def run(self):
        endpoint = "http://archiv.ub.uni-marburg.de/ubfind/OAI/Server"
        shellout("metha-sync -set {set} -format {format} {endpoint}",
                 set=self.set, format=self.format, endpoint=endpoint)
        output = shellout("metha-cat -set {set} -format {format} {endpoint} > {output}",
                          set=self.set,
                          format=self.format,
                          endpoint=endpoint)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(self.path(ext="xml", digest=True))


class MarburgMARC(MarburgTask):
    """
    Convert XML to binary MARC.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='datacite')
    set = luigi.Parameter(default='issn:2196-4270')

    def requires(self):
        return MarburgCombine(format=self.format, set=self.set)

    def run(self):
        output = shellout("python {script} {input} {output}",
                          script=self.assets("73/73_marcbinary.py"), input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(self.path(ext='fincmarc.mrc', digest=True))
