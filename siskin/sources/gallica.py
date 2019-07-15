# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301
#
# Copyright 2019 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
#                   Martin Czygan, <martin.czygan@uni-leipzig.de>
#                   Robert Schenk, <robert.schenk@uni-leipzig.de>
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

Source: Gallica (Bibliothèque nationale de France, BnF)
SID: 20
Ticket: #14793
Origin: OAI
Updates: monthly

"""

import datetime

import luigi

from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask


class GallicaTask(DefaultTask):
    """
    Base task for Gallica.
    """
    TAG = '20'

    def closest(self):
        return monthly(date=self.date)


class GallicaHarvest(GallicaTask):
    """
    Harvest from default http://oai.bnf.fr/oai2/OAIHandler OAI endpoint.
    """
    format = luigi.Parameter(default='oai_dc')
    date = ClosestDateParameter(default=datetime.date.today())
    endpoint = luigi.Parameter(default='http://oai.bnf.fr/oai2/OAIHandler', significant=False)

    def run(self):
        shellout("""metha-sync -rm -format {format} {endpoint} """, endpoint=self.endpoint, format=self.format)
        output = shellout("""metha-cat -format {format} {endpoint} > {output}""",
                          endpoint=self.endpoint,
                          format=self.format)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))


class GallicaMARC(GallicaTask):
    """
    Custom convert to MARC.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='oai_dc')

    def requires(self):
        return GallicaHarvest(date=self.date, format=self.format)

    def run(self):
        output = shellout("python {script} {input} {output}",
                          script=self.assets("20/20_marcbinary.py"),
                          input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='fincmarc.mrc'))
