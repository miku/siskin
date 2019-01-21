# coding: utf-8
# pylint: disable=C0301,C0330,W0622,E1101

# Copyright 2018 by Leipzig University Library, http://ub.uni-leipzig.de
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
Data sharing with SLUB Dresden.

[slub]

ftp-username = username
ftp-password = password
ftp-host = sftp://data.srv.de
ftp-path = /target_data
ftp-pattern = *

"""

import datetime

import luigi

from gluish.format import TSV
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.benchmark import timed
from siskin.common import FTPMirror
from siskin.task import DefaultTask


class SLUBTask(DefaultTask):
    """
    Base task for SLUB related tasks.
    """
    TAG = 'slub'

    def closest(self):
        return weekly(date=self.date)

class SLUBPaths(SLUBTask):
    """
    Mirror SLUB via FTP.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        host = self.config.get('slub', 'ftp-host')
        username = self.config.get('slub', 'ftp-username')
        password = self.config.get('slub', 'ftp-password')
        base = self.config.get('slub', 'ftp-path')
        pattern = self.config.get('slub', 'ftp-pattern')
        return FTPMirror(host=host, username=username, password=password,
                         base=base, pattern=pattern)

    @timed
    def run(self):
        output = shellout('sort {input} > {output}', input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="filelist"), format=TSV)
