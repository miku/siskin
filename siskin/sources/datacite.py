# coding: utf-8
# pylint: disable=C0301,E1101

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
datacite.org

> Locate, identify, and cite research data with the leading global provider of DOIs for research data.

----

Config:

[core]

metha-dir = /path/to/dir


"""

import datetime

import luigi
from gluish.common import Executable
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.task import DefaultTask


class DataciteTask(DefaultTask):
    """
    Base task.
    """
    def closest(self):
        return monthly(date=self.date)

class DataciteCombine(DataciteTask):
    """
    Single file dump.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://oai.datacite.org/oai", significant=False)
    prefix = luigi.Parameter(default="oai_dc", significant=False)

    def requires(self):
        return Executable(name='metha-sync', message='https://github.com/miku/metha')

    def run(self):
        shellout("METHA_DIR={dir} metha-sync -format {prefix} {url}",
                 prefix=self.prefix, url=self.url, dir=self.config.get('core', 'metha-dir'))
        output = shellout("METHA_DIR={dir} metha-cat -root Records -format {prefix} {url} | pigz -c > {output}",
                          prefix=self.prefix, url=self.url, dir=self.config.get('core', 'metha-dir'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="xml.gz"))

class DataciteIntermediateSchema(DataciteTask):
    """
    Experimental.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return DataciteCombine(date=self.date)

    def run(self):
        mapdir = 'file:///%s' % self.assets("maps/")
        output = shellout("""flux.sh {flux} in={input} MAP_DIR={mapdir} | pigz -c > {output}""",
                          flux=self.assets("datacite/flux.flux"), mapdir=mapdir, input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))

class DataciteExport(DataciteTask):
    """
    Export to various formats
    """
    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='solr5vu3', description='export format')

    def requires(self):
        return DataciteIntermediateSchema(date=self.date)

    def run(self):
        output = shellout("span-export -o {format} <(unpigz -c {input}) | pigz -c > {output}", format=self.format, input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        extensions = {
            'solr5vu3': 'ldj.gz',
            'formeta': 'form.gz',
        }
        return luigi.LocalTarget(path=self.path(ext=extensions.get(self.format, 'gz')))
