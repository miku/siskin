#!/usr/bin/env python
# coding: utf-8
#
#  Copyright 2016 by Leipzig University Library, http://ub.uni-leipzig.de
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
#

"""
Journaltocs API. http://www.journaltocs.ac.uk

26,120 journal TOCs (2,728 publishers).

[journaltocs]

user = theuserid
"""

from gluish.utils import shellout
from siskin.configuration import Config
from siskin.task import DefaultTask
import luigi

config = Config.instance()

class JournaltocsTask(DefaultTask):
    """
    Base task.
    """
    TAG = 'journaltocs'

class JournaltocsJournalHarvest(JournaltocsTask):
    """
    A sample harvest of the API.
    """
    query = luigi.Parameter(description='query string')

    def run(self):
        output = shellout(""" curl --fail "http://www.journaltocs.ac.uk/api/journals/{query}?user={user}" > {output}""", query=self.query, user=config.get('journaltocs', 'user'))
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True, ext='xml'))

class JournaltocsHarvest(JournaltocsTask, luigi.WrapperTask):
    """
    Harvest a bunch of keywords.
    """

    def requires(self):
        queries = ["*ancient", "*modern*", "*science*", "*engineering*", "*philosophy*", "*medicine*", "*sociology*", "*language*",
                   "*german*", "*english*", "*american*", "*french*", "*library*", "*europe*", "*asia*", "*afric*"]
        for query in queries:
            yield JournaltocsJournalHarvest(query=query)

    def output(self):
        return self.input()
