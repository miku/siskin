# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301

# Copyright 2018 by Leipzig University Library, http://ub.uni-leipzig.de
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
Genderopen OAI.
"""

import datetime

import luigi
from gluish.utils import shellout
from gluish.parameter import ClosestDateParameter
from gluish.format import Gzip
from gluish.intervals import monthly

from siskin.task import DefaultTask


class GenderopenTask(DefaultTask):
    """
    Base task for gender open.
    """
    TAG = '162'
    REPO = "https://www.genderopen.de/oai/request"

    def closest(self):
        return monthly(self.date)


class GenderopenIntermediateSchema(GenderopenTask):
    """
    Harvest and convert.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("metha-sync {repo} && metha-cat {repo} | span-import -i genderopen | gzip -c > {output}",
                          repo=self.REPO)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)
