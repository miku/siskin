# coding: utf-8
# pylint: disable=C0301

# Copyright 2016 by Leipzig University Library, http://ub.uni-leipzig.de
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
Genios
------

* References (FZS)
* Fulltexts (various packages)

* Dump (called reload, monthly)
* Updates

"""

import datetime
import operator
import os
import re

import luigi
from gluish.format import TSV
from gluish.utils import shellout

from siskin.common import Executable
from siskin.task import DefaultTask
from siskin.utils import iterfiles


class GeniosTask(DefaultTask):
    """
    Genios task.
    """
    TAG = 'genios'

class GeniosDropbox(GeniosTask):
    """
    Pull down content.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return Executable('rsync', message='https://rsync.samba.org/')

    def run(self):
        target = os.path.join(self.taskdir(), 'mirror')
        shellout("mkdir -p {target} && rsync {rsync_options} {src} {target}",
                 rsync_options=self.config.get('gbi', 'rsync-options', '-avzP'),
                 src=self.config.get('gbi', 'scp-src'), target=target)

        if not os.path.exists(self.taskdir()):
            os.makedirs(self.taskdir())

        with self.output().open('w') as output:
            for path in iterfiles(target):
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)

class GeniosLatestReload(GeniosTask):
    """
    The lastest reload files.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return GeniosDropbox(date=self.date)

    def run(self):
        files = []

        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                match = re.search(r'konsortium_sachsen_([a-z_]*)_([A-Z]{2,4})_reload_(?P<date>[0-9]{6,6}).zip', row.path)
                if match:
                    date = match.groupdict().get('date')
                    files.append((date, row.path))

        latestdate = max(files, key=operator.itemgetter(0))[0]
        latestfiles = []
        for date, path in files:
            if date == latestdate:
                latestfiles.append(path)

        with self.output().open('w') as output:
            for path in latestfiles:
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)

class GeniosReload(GeniosTask):
    """
    The lastest reload files.
    """
    tag = luigi.Parameter(default='201606', description='date tag given by gbi, e.g. 201606')

    def requires(self):
        return GeniosDropbox()

    def run(self):
        files = []

        with self.output().open('w') as output:
            with self.input().open() as handle:
                for row in handle.iter_tsv(cols=('path',)):
                    match = re.search(r'konsortium_sachsen_([a-z_]*)_([A-Z]{2,4})_reload_(?P<date>[0-9]{6,6}).zip', row.path)
                    if match:
                        date = match.groupdict().get('date')
                        if date == self.tag:
                            output.write_tsv(row.path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)
