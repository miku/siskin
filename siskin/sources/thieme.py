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
from gluish.intervals import monthly
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
        return monthly(date=self.date)

class ThiemeCombine(ThiemeTask):
    """ Combine files."""

    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default=config.get('thieme', 'oai'), significant=False)
    prefix = luigi.Parameter(default="tm")
    collection = luigi.Parameter(default='journalarticles')

    def requires(self):
        return Executable(name='metha-sync', message='https://github.com/miku/metha')

    def run(self):
        shellout("METHA_DIR={dir} metha-sync -format {prefix} {url}", prefix=self.prefix, url=self.url, dir=config.get('core', 'metha-dir'))
        output = shellout("METHA_DIR={dir} metha-cat -format {prefix} {url} | pigz -c > {output}", prefix=self.prefix, url=self.url, dir=config.get('core', 'metha_dir'))
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="xml.gz"))
