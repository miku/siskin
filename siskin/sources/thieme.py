# coding: utf-8

#  Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
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

"""
Config:

[core]

metha-dir = /path/to/dir

[thieme]

oai = https://example.com/oai/provider
"""

from gluish.common import Executable
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.configuration import Config
from siskin.task import DefaultTask
import datetime
import luigi

config = Config.instance()

class ThiemeTask(DefaultTask):
    """ Thieme connect. """
    TAG = 'thieme'

    def closest(self):
        return weekly(date=self.date)

class ThiemeCombine(ThiemeTask):
    """ Combine files."""

    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default=config.get('thieme', 'oai'), significant=False)
    prefix = luigi.Parameter(default="tm")
    set = luigi.Parameter(default='journalarticles')

    def requires(self):
        return Executable(name='metha-sync', message='https://github.com/miku/metha')

    def run(self):
        shellout("METHA_DIR={dir} metha-sync -set {set} -format {prefix} {url}", set=self.set, prefix=self.prefix, url=self.url, dir=config.get('core', 'metha-dir'))
        output = shellout("METHA_DIR={dir} metha-cat -format {prefix} {url} | pigz -c > {output}", prefix=self.prefix, url=self.url, dir=config.get('core', 'metha-dir'))
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="xml.gz"))

class ThiemeIntermediateSchema(ThiemeTask):
    """
    Conv-oo-do-ert.
    """

    date = ClosestDateParameter(default=datetime.date.today())
    set = luigi.Parameter(default='journalarticles')

    def requires(self):
        return ThiemeCombine(date=self.date, prefix='tm', set=self.set)

    def run(self):
        output = shellout("span-import -i thieme-tm <(unpigz -c {input}) | pigz -c > {output}", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="ldj.gz"))

class ThiemeSolr(ThiemeTask):
    """
    Create something solr importable. Attach a single ISIL to all records.

    Ad-hoc task, with all records attached.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    set = luigi.Parameter(default='journalarticles')
    version = luigi.Parameter(default='solr5vu3v11', description='export JSON flavors, e.g.: solr4vu13v{1,10}, solr5vu3v11')

    def requires(self):
        return ThiemeIntermediateSchema(date=self.date, set=self.set)

    def run(self):
        output = shellout("""span-tag -c <(echo '{{"DE-15": {{"any": {{}}}}}}') <(unpigz -c {input}) > {output}""", input=self.input().path)
        output = shellout("span-export -o {version} {input} | pigz -c > {output}", input=output, version=self.version)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))
