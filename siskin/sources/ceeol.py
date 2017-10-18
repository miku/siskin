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
https://www.ceeol.com/, refs #9398.

----

Config:

[ceeol]

disk-dir = /path/to/disk


"""

from __future__ import print_function

import glob
import os
import tempfile

import luigi
from gluish.utils import shellout

from siskin.task import DefaultTask


class CeeolTask(DefaultTask):
    """
    Base task.
    """
    TAG = '53'


class CeeolJournalsDumpIntermediateSchema(CeeolTask):
    """
    Combine all journals from disk dump and convert them to intermediate schema.
    """

    def run(self):
        diskdir = self.config.get('ceeol', 'disk-dir')
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for path in glob.glob(os.path.join(diskdir, '1Journal-Articles-XML', 'articles_*xml')):
            shellout("span-import -i ceeol {input} | pigz -c >> {output}", input=path, output=stopover)
        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="ldj.gz"))
