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
Config:

[core]

metha-dir = /path/to/dir

[thieme]

oai = https://example.com/oai/provider
"""

import datetime
import tempfile

import luigi
from gluish.common import Executable
from gluish.format import TSV
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.decorator import deprecated
from siskin.sources.amsl import AMSLFilterConfig
from siskin.task import DefaultTask


class ThiemeTask(DefaultTask):
    """ Thieme connect. """
    TAG = '60'

    def closest(self):
        return weekly(date=self.date)


class ThiemeCombine(ThiemeTask):
    """ Combine files."""

    date = ClosestDateParameter(default=datetime.date.today())
    prefix = luigi.Parameter(default="nlm")
    set = luigi.Parameter(default='journalarticles')

    def requires(self):
        return Executable(name='metha-sync', message='https://github.com/miku/metha')

    def run(self):
        url = self.config.get('thieme', 'oai')
        shellout("METHA_DIR={dir} metha-sync -set {set} -format {prefix} {url}",
                 set=self.set, prefix=self.prefix, url=url, dir=self.config.get('core', 'metha-dir'))
        output = shellout("METHA_DIR={dir} metha-cat -format {prefix} {url} | pigz -c > {output}",
                          prefix=self.prefix, url=url, dir=self.config.get('core', 'metha-dir'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="xml.gz"))


class ThiemeIntermediateSchema(ThiemeTask):
    """
    Conv-oo-do-ert.
    """

    date = ClosestDateParameter(default=datetime.date.today())
    set = luigi.Parameter(default='journalarticles')

    def requires(self):
        return ThiemeCombine(date=self.date, prefix='nlm', set=self.set)

    def run(self):
        output = shellout(
            "span-import -i thieme-nlm <(unpigz -c {input}) | pigz -c > {output}", input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="ldj.gz"))


class ThiemeExport(ThiemeTask):
    """
    Create something solr importable. Attach a single ISIL to all records.

    Ad-hoc task, with all records attached.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    set = luigi.Parameter(default='journalarticles')
    version = luigi.Parameter(
        default='solr5vu3', description='export JSON flavors, e.g.: solr4vu13v{1,10}, solr5vu3v11')

    def requires(self):
        return {
            'is': ThiemeIntermediateSchema(date=self.date, set=self.set),
            'config': AMSLFilterConfig(date=self.date),
        }

    def run(self):
        output = shellout("span-tag -c {config} <(unpigz -c {input}) | pigz -c > {output}",
                          config=self.input().get('config').path, input=self.input().get('is').path)
        output = shellout(
            "span-export -o {version} <(unpigz -c {input}) | pigz -c > {output}", input=output, version=self.version)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))


class ThiemeISSNList(ThiemeTask):
    """
    A list of Elsevier journals DOIs.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return {'input': ThiemeIntermediateSchema(date=self.date),
                'jq': Executable(name='jq', message='https://github.com/stedolan/jq')}

    def run(self):
        _, output = tempfile.mkstemp(prefix='siskin-')
        shellout("""jq -c -r '.["rft.issn"][]?' <(unpigz -c {input}) >> {output} """, input=self.input(
        ).get('input').path, output=output)
        shellout("""jq -c -r '.["rft.eissn"][]?' <(unpigz -c {input}) >> {output} """, input=self.input(
        ).get('input').path, output=output)
        output = shellout("""sort -u {input} > {output} """, input=output)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
