# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301
#
#  Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                 by The Finc Authors, http://finc.info
#                 by Martin Czygan, <martin.czygan@uni-leipzig.de>
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
OPIV.

Configuration keys:

[opiv]

src = [user@server:[port]]/path/to/directory
"""

from gluish.format import TSV
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.common import Directory
from siskin.configuration import Config
from siskin.task import DefaultTask
from siskin.utils import iterfiles
import datetime
import luigi

config = Config.instance()

class OpivTask(DefaultTask):
    TAG = '031'

class OpivSync(OpivTask):
    """ Just copy over the marc files. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return Directory(path=self.taskdir())

    def run(self):
        shellout("rsync -avz {src} {dst}", src=config.get('opiv', 'src'), dst=self.taskdir())
        with self.output().open('w') as output:
            for path in iterfiles(self.taskdir(), fun=lambda p: '-luigi-tmp-' not in p):
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)
