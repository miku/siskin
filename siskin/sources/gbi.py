#!/usr/bin/env python
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
GBI publisher.

[gbi]

ftp-host = host.name
ftp-username = username
ftp-password = password
ftp-path = /
ftp-pattern = some*glob*pattern.zip

scp-src = username@ftp.example.de:/home/gbi
"""

from gluish.format import TSV
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.benchmark import timed
from siskin.common import FTPMirror, Executable
from siskin.configuration import Config
from siskin.task import DefaultTask
from siskin.utils import iterfiles
import datetime
import luigi
import os
import tempfile
import zipfile

config = Config.instance()

class GBITask(DefaultTask):
    TAG = '048'

    def closest(self):
        return datetime.date(2015, 6, 1)

class GBIDropbox(GBITask):
    """
    Pull down GBI dropbox content.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return Executable('scp', message='https://en.wikipedia.org/wiki/Secure_copy')

    def run(self):
        target = os.path.join(self.taskdir(), 'mirror')
        shellout("mkdir -p {target} && rsync -avzP {src} {target}", src=config.get('gbi', 'scp-src'), target=target)
        if not os.path.exists(self.taskdir()):
            os.makedirs(self.taskdir())
        with self.output().open('w') as output:
            for path in iterfiles(target):
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)

class GBIZipWithZips(GBITask):
    """
    List of files, that contains itself zip files.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return GBIDropbox(date=self.date)

    def run(self):
        with self.input().open() as handle:
            with self.output().open('w') as output:
                for row in handle.iter_tsv(cols=('path',)):
                    if row.path.endswith('.zip'):
                        try:
                            zf = zipfile.ZipFile(row.path)
                            if any([fn.endswith(".zip") for fn in zf.namelist()]):
                                output.write_tsv(row.path)
                        except zipfile.BadZipfile as err:
                            self.logger.warn("%s: %s" % (row.path, err))

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)


#
# Below tasks are DEPRECATED and will be removed shortly.
#

class GBIGroup(GBITask):
    """ Find groups (subdirs). These are not part of the metadata. """
    date = ClosestDateParameter(default=datetime.date.today())
    group = luigi.Parameter(default='wiwi', description='wiwi, fzs, sowi, recht')

    def requires(self):
        return GBISync(date=self.date)

    def run(self):
        with self.input().open() as handle:
            with self.output().open('w') as output:
                for row in handle.iter_tsv(cols=('path',)):
                    group = row.path.split('/')[-2]
                    if group == self.group:
                        output.write_tsv(row.path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'), format=TSV)

class GBIXMLGroup(GBITask):
    """
    Extract all XML files and concat them. Inject a <x-group> tag, which
    carries the "collection" from the directory name into the metadata.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    group = luigi.Parameter(default='wiwi', description='wiwi, fzs, sowi, recht')

    def requires(self):
       return GBIGroup(date=self.date, group=self.group)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                shellout("""unzip -p {path} \*.xml 2> /dev/null |
                            iconv -f iso-8859-1 -t utf-8 |
                            LC_ALL=C grep -v "^<\!DOCTYPE GENIOS PUBLIC" |
                            LC_ALL=C sed -e 's@<?xml version="1.0" encoding="ISO-8859-1" ?>@@g' |
                            LC_ALL=C sed -e 's@</Document>@<x-group>{group}</x-group></Document>@' >> {output}""",
                         output=stopover, path=row.path, group=self.group,
                         ignoremap={1: 'OK', 9: 'ignoring broken zip'})
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'), format=TSV)

class GBIXML(GBITask):
    """
    Extract all XML files and concat them. Inject a <x-group> tag, which
    carries the "collection" from the directory name into the metadata.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        """ Note: ingore broken 'sowi' for now. """
        return [GBIXMLGroup(date=self.date, group=group) for group in ['wiwi', 'fzs', 'recht']]

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'), format=TSV)

class GBIIntermediateSchema(GBITask):
    """
    Convert GBI to intermediate format via span.
    If group is 'all', create intermediate schema of all groups.
    """

    date = ClosestDateParameter(default=datetime.date.today())
    group = luigi.Parameter(default='all', description='wiwi, fzs, sowi, recht')

    def requires(self):
        span = Executable(name='span-import', message='http://git.io/vI8NV')
        if self.group == "all":
            return {'span': span, 'file': GBIXML(date=self.date)}
        else:
            return {'span': span, 'file': GBIXMLGroup(date=self.date, group=self.group)}

    @timed
    def run(self):
        output = shellout("span-import -i genios {input} > {output}", input=self.input().get('file').path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class GBIISSNList(GBITask):
    """ A list of JSTOR ISSNs. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GBIIntermediateSchema(date=self.date)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout("""jq -r '.["rft.issn"][]' {input} 2> /dev/null >> {output} """, input=self.input().path, output=stopover)
        shellout("""jq -r '.["rft.eissn"][]' {input} 2> /dev/null >> {output} """, input=self.input().path, output=stopover)
        output = shellout("""sort -u {input} > {output} """, input=stopover)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
