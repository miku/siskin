# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

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

import base64
import collections
import datetime
import json
import os
import re
import tempfile

import luigi
from gluish.common import Executable
from gluish.format import TSV, Gzip
from gluish.intervals import weekly
from gluish.utils import shellout

from BeautifulSoup import BeautifulStoneSoup
from siskin.benchmark import timed
from siskin.common import FTPMirror
from siskin.sources.amsl import AMSLFilterConfig
from siskin.task import DefaultTask
from siskin.utils import iterfiles


class ElsevierJournalsTask(DefaultTask):
    """ Elsevier journals base. """
    TAG = '085'

class ElsevierJournalsBacklogIntermediateSchema(ElsevierJournalsTask):
    """
    Convert backlog to intermediate schema.
    """
    def run(self):
        directory = self.config.get('elsevierjournals', 'backlog-dir')
        _, output = tempfile.mkstemp(prefix='siskin-')
        for path in sorted(iterfiles(directory, fun=lambda p: p.endswith('.tar'))):
            shellout("span-import -i elsevier-tar {input} | pigz -c >> {output}", input=path, output=output, pipefail=True)
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
        return FTPMirror(host=self.config.get('elsevierjournals', 'ftp-host'),
                         username=self.config.get('elsevierjournals', 'ftp-username'),
                         password=self.config.get('elsevierjournals', 'ftp-password'),
                         pattern=self.config.get('elsevierjournals', 'ftp-pattern'),
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
            for row in sorted(handle.iter_tsv(cols=('path',))):
                if not row.path.endswith('.tar'):
                    continue
                shellout("span-import -i elsevier-tar {input} | pigz -c >> {output}", input=row.path, output=output, pipefail=True)
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

class ElsevierJournalsDOIList(ElsevierJournalsTask):
    """
    A list of Elsevier journals DOIs.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return {'input': ElsevierJournalsIntermediateSchema(date=self.date),
                'jq': Executable(name='jq', message='https://github.com/stedolan/jq')}

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        # process substitution sometimes results in a broken pipe, so extract beforehand
        output = shellout("unpigz -c {input} > {output}", input=self.input().get('input').path)
        shellout("""jq -r '.doi?' {input} | grep -o "10.*" 2> /dev/null | LC_ALL=C sort -S50% > {output} """,
                 input=output, output=stopover, pipefail=True)
        os.remove(output)
        output = shellout("""sort -S50% -u {input} > {output} """, input=stopover)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class ElsevierJournalsISSNList(ElsevierJournalsTask):
    """
    A list of Elsevier journals DOIs.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return {'input': ElsevierJournalsIntermediateSchema(date=self.date),
                'jq': Executable(name='jq', message='https://github.com/stedolan/jq')}

    @timed
    def run(self):
        _, output = tempfile.mkstemp(prefix='siskin-')
        shellout("""jq -c -r '.["rft.issn"][]?' <(unpigz -c {input}) >> {output} """, input=self.input().get('input').path, output=output, pipefail=True)
        shellout("""jq -c -r '.["rft.eissn"][]?' <(unpigz -c {input}) >> {output} """, input=self.input().get('input').path, output=output, pipefail=True)
        output = shellout("""sort -u {input} > {output} """, input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class ElsevierJournalsExport(ElsevierJournalsTask):
    """
    A SOLR-importable version of Elsevier.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    version = luigi.Parameter(default='solr5vu3v11', description='export JSON flavors, e.g.: solr4vu13v{1,10}, solr5vu3v11')

    def requires(self):
        return {
            'is': ElsevierJournalsIntermediateSchema(date=self.date),
            'config': AMSLFilterConfig(date=self.date)
        }

    @timed
    def run(self):
        output = shellout("span-tag -c {config} <(unpigz -c {input}) | pigz -c > {output}",
                          config=self.input().get('config').path, input=self.input().get('is').path, pipefail=True)
        output = shellout("span-export -o {version} <(unpigz -c {input}) | pigz -c > {output}", input=output, version=self.version, pipefail=True)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))
