# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

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
Elsevier jounrals. Refs. #6975.

Configuration keys:

[elsevierjournals]

ftp-host = sftp://host.name
ftp-username = username
ftp-password = password
ftp-path = /
ftp-pattern = *

backlog-dir = /path/to/dir

"""

from BeautifulSoup import BeautifulStoneSoup
from gluish.common import Executable
from gluish.format import TSV, Gzip
from gluish.intervals import weekly
from gluish.utils import shellout
from siskin.benchmark import timed
from siskin.common import FTPMirror
from siskin.configuration import Config
from siskin.task import DefaultTask
from siskin.utils import iterfiles
import base64
import collections
import datetime
import json
import luigi
import os
import re
import tempfile

config = Config.instance()

class ElsevierJournalsTask(DefaultTask):
    """ Elsevier journals base. """
    TAG = '085'

class ElsevierJournalsBacklogIntermediateSchema(ElsevierJournalsTask):
    """
    Convert backlog to intermediate schema.
    """
    def run(self):
        directory = config.get('elsevierjournals', 'backlog-dir')
        _, output = tempfile.mkstemp(prefix='siskin-')
        for path in iterfiles(directory, fun=lambda p: p.endswith('.tar')):
            shellout("span-import -i elsevier-tar {input} | pigz -c >> {output}", input=path, output=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)

class ElsevierJournalsPaths(ElsevierJournalsTask):
    """
    Sync.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    max_retries = luigi.IntParameter(default=10, significant=False)
    timeout = luigi.IntParameter(default=20, significant=False, description='timeout in seconds')

    def requires(self):
        return FTPMirror(host=config.get('elsevierjournals', 'ftp-host'),
                         username=config.get('elsevierjournals', 'ftp-username'),
                         password=config.get('elsevierjournals', 'ftp-password'),
                         pattern=config.get('elsevierjournals', 'ftp-pattern'),
                         max_retries=self.max_retries,
                         timeout=self.timeout)

    @timed
    def run(self):
        self.input().move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class ElsevierJournalsUpdatesIntermediateSchema(ElsevierJournalsTask):
    """
    Intermediate schema from updates.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return ElsevierJournalsPaths(date=self.date)

    @timed
    def run(self):
        _, output = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                if not row.path.endswith('.tar'):
                    continue
                shellout("span-import -i elsevier-tar {input} | pigz -c >> {output}", input=row.path, output=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)

class ElsevierJournalsIntermediateSchema(ElsevierJournalsTask):
    """ Combine backlog and updates. """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return [ElsevierJournalsBacklogIntermediateSchema(),
                ElsevierJournalsUpdatesIntermediateSchema(date=self.date)]

    @timed
    def run(self):
        _, output = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path, output=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)

class ElsevierJournalsSolr(ElsevierJournalsTask):
    """
    Create something solr importable. Attach a single ISIL to all records.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return ElsevierJournalsIntermediateSchema(tag=self.tag)

    @timed
    def run(self):
        output = shellout("""span-tag -c <(echo '{{"DE-15": {{"any": {{}}}}}}') {input} > {output}""", input=self.input().path)
        output = shellout("span-export {input} > {output}", input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))
