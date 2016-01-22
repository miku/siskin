# coding: utf-8
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
DeGruyter task.

[degruyter]

ftp-host = host.name
ftp-username = username
ftp-password = password
ftp-path = /
ftp-pattern = some*glob*pattern.zip

"""

from siskin.benchmark import timed
from siskin.common import FTPMirror, Executable
from gluish.format import TSV
from gluish.intervals import daily
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.utils import iterfiles
from siskin.configuration import Config
from siskin.task import DefaultTask
import datetime
import luigi
import re
import shutil
import tempfile

config = Config.instance()

class DegruyterTask(DefaultTask):
    TAG = 'degruyter'

    def closest(self):
        return datetime.date(2015, 4, 1)

class DegruyterPaths(DegruyterTask):
    """ A list of Degruyter ile paths (via FTP). """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        host = config.get('degruyter', 'ftp-host')
        username = config.get('degruyter', 'ftp-username')
        password = config.get('degruyter', 'ftp-password')
        base = config.get('degruyter', 'ftp-path')
        pattern = config.get('degruyter', 'ftp-pattern')
        exclude_glob = config.get('degruyter', 'ftp-exclude-glob', '')
        return FTPMirror(host=host, username=username, password=password,
                         base=base, pattern=pattern, exclude_glob=exclude_glob)

    @timed
    def run(self):
        self.input().move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="filelist"), format=TSV)

class DegruyterMembers(DegruyterTask):
    """ Extract a full list of archive members. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return DegruyterPaths(date=self.date)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                shellout(""" unzip -l {input} | grep "xml$" | awk '{{print "{input}\t"$4}}' >> {output} """,
                         preserve_whitespace=True, input=row.path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class DegruyterXML(DegruyterTask):
    """
    Extract all XML files from Jstor dump. TODO(miku): Check all subdirs, not just SSH.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    group = luigi.Parameter(default='SSH', description='Nationallizenz_Zeitschriften, Nationallizenz_Jahrbuecher')

    def requires(self):
        return DegruyterPaths(date=self.date)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                if not '/%s/' % self.group in row.path:
                    continue
                shellout("unzip -p {path} \*.xml 2> /dev/null >> {output}", output=stopover, path=row.path,
                         ignoremap={1: 'OK', 9: 'skip corrupt file'})
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'), format=TSV)

class DegruyterIntermediateSchema(DegruyterTask):
    """ Convert to intermediate format via span. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
       return {'span': Executable(name='span-import', message='http://git.io/vI8NV'),
               'file': DegruyterXML(date=self.date)}

    @timed
    def run(self):
        output = shellout("span-import -i degruyter {input} | pigz -c > {output}", input=self.input().get('file').path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))

class DegruyterISSNList(DegruyterTask):
    """ List of ISSNs. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
       return DegruyterIntermediateSchema(date=self.date)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout("""jq -r '.["rft.issn"][]' <(unpigz -c {input}) 2> /dev/null >> {output} """, input=self.input().path, output=stopover)
        shellout("""jq -r '.["rft.eissn"][]' <(unpigz -c {input}) 2> /dev/null >> {output} """, input=self.input().path, output=stopover)
        output = shellout("""sort -u {input} > {output} """, input=stopover)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class DegruyterDOIList(DegruyterTask):
    """ A list of Degryter DOIs. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'input': DegruyterIntermediateSchema(date=self.date),
                'jq': Executable(name='jq', message='https://github.com/stedolan/jq')}

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout("""jq -r '.doi' <(unpigz -c {input}) | grep -v "null" | grep -o "10.*" 2> /dev/null > {output} """, input=self.input().get('input').path, output=stopover)
        output = shellout("""sort -u {input} > {output} """, input=stopover)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
