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
Highwire, containing data from (approximate counts):

1764237 Oxford University Press
 558404 BMJ Publishing Group Ltd
  42011 Company of Biologists
  25004 Duke University Press
  15971 Cold Spring Harbor Laboratory Press
   5945 British Medical Journal Publishing Group
   1212 The Company of Biologists Ltd
    149 eLife Sciences Publications Limited
     81 The Company of Biologists

"""

import datetime

import luigi
from gluish.common import Executable
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.task import DefaultTask


class HighwireTask(DefaultTask):
    """
    Base task.
    """
    TAG = 'highwire'

    def closest(self):
        return weekly(date=self.date)


class HighwireCombine(HighwireTask):
    """
    Single file dump.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="open-archive.highwire.org/handler", significant=False)
    prefix = luigi.Parameter(default="oai_dc", significant=False)

    def requires(self):
        return Executable(name='metha-sync', message='https://github.com/miku/metha')

    def run(self):
        shellout("METHA_DIR={dir} metha-sync -ignore-http-errors -format {prefix} {url}",
                 prefix=self.prefix, url=self.url, dir=self.config.get('core', 'metha-dir'))
        output = shellout("METHA_DIR={dir} metha-cat -format {prefix} {url} | pigz -c > {output}",
                          prefix=self.prefix, url=self.url, dir=self.config.get('core', 'metha-dir'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="xml.gz"))


class HighwireIntermediateSchema(HighwireTask):
    """
    Experimental OAI -> intermediate schema conversion.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="open-archive.highwire.org/handler", significant=False)
    prefix = luigi.Parameter(default="oai_dc", significant=False)

    def requires(self):
        return HighwireCombine(date=self.date, url=self.url, prefix=self.prefix)

    def run(self):
        output = shellout("unpigz -c {input} | span-import -i highwire | pigz -c > {output}",
                          input=self.input().path, pipefail=False)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="ldj.gz"))


class HighwireExport(HighwireTask):
    """
    Export to various formats
    """
    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='solr5vu3', description='export format')

    def requires(self):
        return HighwireIntermediateSchema(date=self.date)

    def run(self):
        output = shellout("span-export -o {format} <(unpigz -c {input}) | pigz -c > {output}", format=self.format, input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        extensions = {
            'solr5vu3': 'ldj.gz',
            'formeta': 'form.gz',
        }
        return luigi.LocalTarget(path=self.path(ext=extensions.get(self.format, 'gz')))
