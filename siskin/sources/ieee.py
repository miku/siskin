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
IEEE. (http://www.ieee.org/documents/xmldoc_ver5.5.pdf)

Config
------

[ieee]

ftp-host = host.name
ftp-username = username
ftp-password = password
ftp-path = /
ftp-pattern = *

# initial shipment compressed as tar.gz
backlog-archive = /path/to/ieee.tar.gz

"""

import datetime
import tempfile

import luigi

from gluish.format import TSV
from gluish.utils import shellout
from siskin.benchmark import timed
from siskin.common import FTPMirror
from siskin.task import DefaultTask


class IEEETask(DefaultTask):
    TAG = 'ieee'

class IEEEPaths(IEEETask):
    """ A list of IEEE file paths (via FTP). """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        host = self.config.get('ieee', 'ftp-host')
        username = self.config.get('ieee', 'ftp-username')
        password = self.config.get('ieee', 'ftp-password')
        base = self.config.get('ieee', 'ftp-path')
        pattern = self.config.get('ieee', 'ftp-pattern')
        return FTPMirror(host=host, username=username, password=password,
                         base=base, pattern=pattern)

    @timed
    def run(self):
        output = shellout('sort {input} > {output}', input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="filelist"), format=TSV)

class IEEEUpdatesIntermediateSchema(IEEETask):
    """
    Combined updates, with duplicates.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return IEEEPaths(date=self.date)

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                if not 'IEEEUpdates_' in row.path:
                    continue
                shellout(r'unzip -p {input} \*.xml | span-import -i ieee | pigz -c >> {output}', input=row.path, output=stopover)
        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="ldj.gz"))


class IEEEBacklogPaths(IEEETask):
    """
    List files in the backlog. Just a `tar -tf` of the compressed dump.
    """
    def run(self):
        output = shellout('tar -tf {input} > {output}', input=self.config.get('ieee', 'backlog-archive'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="filelist"), format=TSV)

class IEEEBacklogIntermediateSchema(IEEETask):
    """
    Backlog is a single tar file containing an initial set of records.
    """

    def run(self):
        output = shellout("""tar xOzf {input} | span-import -i ieee | pigz -c >> {output}""",
                          input=self.config.get('ieee', 'backlog-archive'), ignoremap={2: 'FIXME later'})
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True, ext='ldj.gz'))

class IEEEIntermediateSchema(IEEETask):
    """
    Combine the backlog and all updates into a single file.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return [IEEEBacklogIntermediateSchema(), IEEEUpdatesIntermediateSchema(date=self.date)]

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path, output=stopover)
        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="ldj.gz"))
