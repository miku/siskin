# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301
#
#  Copyright 2016 by Leipzig University Library, http://ub.uni-leipzig.de
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
Elsevier jounrals. Refs. #6975.

Configuration keys:

[elsevierjournals]

ftp-host = sftp://host.name
ftp-username = username
ftp-password = password
ftp-path = /
ftp-pattern = *

"""

from gluish.common import Executable
from gluish.format import TSV
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from luigi.contrib.esindex import CopyToIndex
from siskin.benchmark import timed
from siskin.common import FTPMirror
from siskin.configuration import Config
from siskin.task import DefaultTask
from siskin.utils import iterfiles
import datetime
import luigi
import os

config = Config.instance()

class ElsevierJournalsTask(DefaultTask):
    """ Jstor base. """
    TAG = '085'

    def closest(self):
        return weekly(self.date)

class ElsevierJournalsPaths(ElsevierJournalsTask):
    """
    Sync.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    max_retries = luigi.IntParameter(default=10, significant=False)
    timeout = luigi.IntParameter(default=20, significant=False, description='timeout in seconds')

    def requires(self):
        return FTPMirror(host=config.get('elsevierjournals', 'ftp-host'),
                         username=config.get('elsevierjournals', 'ftp-username'),
                         password=config.get('elsevierjournals', 'ftp-password'),
                         pattern=config.get('elsevierjournals', 'ftp-pattern'),
                         max_retries=self.max_retries,
                         timeout=self.timeout)

    def run(self):
        self.input().move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class ElsevierJournalsExpand(ElsevierJournalsTask):
    """
    Expand all tar files.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return ElsevierJournalsPaths(date=self.date)

    def run(self):
        shellout("mkdir -p {dir}", dir=self.taskdir())
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                if row.path.endswith('tar'):
                    shellout("tar -xvf {tarfile} -C {dir}", tarfile=row.path, dir=self.taskdir())

        with self.output().open('w') as output:
            for path in iterfiles(self.taskdir()):
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
