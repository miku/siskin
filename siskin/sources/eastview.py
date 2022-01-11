# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301

# Copyright 2022 by Leipzig University Library, http://ub.uni-leipzig.de
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
Eastview.

* refs. #12586

Config
------

[eastview]

ftp-host = host.name
ftp-username = username
ftp-password = password
ftp-path = /
ftp-pattern = *

"""

import datetime
import tempfile

import luigi
from gluish.common import Executable
from gluish.format import TSV
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.benchmark import timed
from siskin.common import FTPMirror
from siskin.task import DefaultTask


class EastViewTask(DefaultTask):
    TAG = 'eastview'

    def closest(self):
        return weekly(date=self.date)


class EastViewPaths(EastViewTask):
    """
    A list of IEEE file paths (via FTP).

    managed-schema
    xmlt.xsl
    xml.zip

    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        host = self.config.get('eastview', 'ftp-host')
        username = self.config.get('eastview', 'ftp-username')
        password = self.config.get('eastview', 'ftp-password')
        base = self.config.get('eastview', 'ftp-path')
        pattern = self.config.get('eastview', 'ftp-pattern')
        return FTPMirror(host=host, username=username, password=password, base=base, pattern=pattern)

    @timed
    def run(self):
        output = shellout('sort {input} > {output}', input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="filelist"), format=TSV)


