# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301
# Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
#                   Tracy Hoffmann, <tracy.hoffmann@uni-leipzig.de>
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
IZI Task #7755, #13652. TODO(miku): move from main to ai.

Config:

[izi]

input = /path/to/izi.xml

"""

import datetime
import os

import luigi

from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from gluish.format import Gzip
from siskin.task import DefaultTask


class IZITask(DefaultTask):
    """ Base task for IZI. """
    TAG = '78'


class IZIMARC(IZITask):
    """
    Convert to binary MARC.
    """
    def run(self):
        output = shellout("""python {script} {input} {output}""",
                         script=self.assets('78/78_marcbinary.py'),
                        input=self.config.get('izi', 'input'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='fincmarc.mrc'))



