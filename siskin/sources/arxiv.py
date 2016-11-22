# coding: utf-8
# pylint: disable=C0301

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
arxiv.org

http://arxiv.org/help/oa/index

----

Config:

[core]

metha-dir = /path/to/dir

----

From: https://groups.google.com/forum/#!topic/arxiv-api/aOacIt6KD2E

> the problem is that the API is not the appropriate vehicle to obtain a full
  copy of arXiv metadata. It is intended for interactive queries and search overlays
  and should not be used for full replication. The size of result sets is capped at a large but finite number.
  You should use OAI-PMH instead for your purposes.

"""

import datetime

import luigi

from gluish.common import Executable
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.benchmark import timed
from siskin.task import DefaultTask


class ArxivTask(DefaultTask):
    """
    Base task.
    """
    def closest(self):
        return weekly(date=self.date)

class ArxivCombine(ArxivTask):
    """
    Single file dump.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://export.arxiv.org/oai2", significant=False)
    prefix = luigi.Parameter(default="oai_dc", significant=False)

    def requires(self):
        return Executable(name='metha-sync', message='https://github.com/miku/metha')

    def run(self):
        shellout("METHA_DIR={dir} metha-sync -format {prefix} {url}",
                 prefix=self.prefix, url=self.url, dir=self.config.get('core', 'metha-dir'))
        output = shellout("METHA_DIR={dir} metha-cat -format {prefix} {url} | pigz -c > {output}",
                          prefix=self.prefix, url=self.url, dir=self.config.get('core', 'metha-dir'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="xml.gz"))

class ArxivIntermediateSchema(ArxivTask):
    """
    Experimental. Not a valid schema yet.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://export.arxiv.org/oai2", significant=False)
    prefix = luigi.Parameter(default="oai_dc", significant=False)

    def requires(self):
        return ArxivCombine(date=self.date, url=self.url)

    @timed
    def run(self):
        output = shellout("span-import -i oai <(unpigz -c {input}) | pigz -c > {output}", input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="xml.gz"))

class ArxivExport(ArxivTask):
    """
    Export to various formats
    """
    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='solr5vu3', description='export format')

    def requires(self):
        return ArxivIntermediateSchema(date=self.date)

    def run(self):
        output = shellout("span-export -o {format} <(unpigz -c {input}) | pigz -c > {output}", format=self.format, input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        extensions = {
            'solr5vu3': 'ldj.gz',
            'formeta': 'form.gz',
        }
        return luigi.LocalTarget(path=self.path(ext=extensions.get(self.format, 'gz')))
