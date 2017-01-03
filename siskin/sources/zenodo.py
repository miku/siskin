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
Zenodo test task for generic OAI.
"""

import datetime

import luigi

from siskin.oai import OAIIntermediateSchema
from siskin.task import DefaultTask


class ZenodoTask(DefaultTask):
    TAG = 'zenodo'


class ZenodoIntermediateSchema(ZenodoTask, luigi.WrapperTask):
    """ Convert Zenodo to intermediate schema. """
    date = luigi.DateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="https://zenodo.org/oai2d", significant=False)
    prefix = luigi.Parameter(default="oai_dc", significant=False)

    sid = luigi.Parameter(default='zenodo', description="internal data source id")
    collection = luigi.Parameter(default='Zenodo', description="internal collection name")
    format = luigi.Parameter(default='ElectronicArticle')

    def requires(self):
        return OAIIntermediateSchema(date=self.date, url=self.url, prefix=self.prefix, sid='zenodo', collection='Zenodo', format=self.format)

    def output(self):
        return self.input()
