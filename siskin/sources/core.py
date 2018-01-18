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
https://core.ac.uk/

Aggregating the world's open access research papers.

We offer seamless access to millions of open access research papers, enrich the
collected data for text-mining and provide unique services to the research
community.

CORE aggregates research papers from data providers from all over the world
including institutional repositories, subject-repositories and journal
publishers. This process, which is called harvesting, allows us to offer search,
text-mining and analytical capabilities over not only metadata, but also the
full-text of the research papers making CORE a unique service in the research
community.

CORE currently contains 83,326,016 open access articles, from over tens of
thousands journals, collected from over 3,667 repositories around the world. 

----

* https://scholarlycommunications.jiscinvolve.org/wp/2017/12/14/enhancing-library-discovery-services-with-core-content/

> The CORE service is working in partnership with ProQuest to deliver more
> content within their library discovery services (Ex Libris Primo and Ex Libris
> Summon).
"""

from __future__ import print_function

import datetime

import luigi
from gluish.utils import shellout
from gluish.parameter import ClosestDateParameter

from siskin.task import DefaultTask


class CoreTask(DefaultTask):
    """
    Base task. Irregular updates.
    """
    TAG = 'core'

    def closest(self):
        return datetime.date(2017, 11, 1)


class CoreDownload(CoreTask):
    """
    Metadata. Filenaming inconsistent (tar or tar.gz).
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("""curl --fail https://core.ac.uk/datasets/core_{date}_metadata.tar > {output}""",
                          date=self.closest().strftime("%Y-%m-%d"))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="tar.gz"))

class CoreDownloadFulltext(CoreTask):
    """
    Metadata and Fulltext. Filenaming inconsistent (tar or tar.gz).
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("""curl --fail https://core.ac.uk/datasets/core_{date}_fulltext.tar > {output}""",
                          date=self.closest().strftime("%Y-%m-%d"))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="tar.gz"))

