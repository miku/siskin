# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301
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
[jstor]

ftp-host = host.name
ftp-username = username
ftp-password = password
ftp-path = /
ftp-pattern = *
"""

from gluish.format import TSV
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.benchmark import timed
from siskin.common import FTPMirror, Executable
from siskin.configuration import Config
from siskin.task import DefaultTask
from siskin.utils import nwise
import datetime
import itertools
import luigi
import pipes
import tempfile

config = Config.instance()

class JstorTask(DefaultTask):
    """ Jstor base. """
    TAG = 'jstor'

    def closest(self):
        return weekly(self.date)

class JstorPaths(JstorTask):
    """ Sync. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return FTPMirror(host=config.get('jstor', 'ftp-host'),
                         username=config.get('jstor', 'ftp-username'),
                         password=config.get('jstor', 'ftp-password'),
                         pattern=config.get('jstor', 'ftp-pattern'))

    def run(self):
        self.input().move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class JstorMembers(JstorTask):
    """ Extract a full list of archive members. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return JstorPaths(date=self.date)

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

class JstorLatestMembers(JstorTask):
    """ Find the latest archive members for all article ids. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return JstorMembers(date=self.date)

    @timed
    def run(self):
        """ Expect input to be sorted, so tac will actually be a perfect rewind. """
        output = shellout("tac {input} | sort -S 50% -u -k2,2 | sort -S 50% -k1,1 > {output}", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class JstorXML(JstorTask):
    """
    Create an latest snapshot of the latest data.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    batch = luigi.IntParameter(default=512, significant=False)

    def requires(self):
        return JstorLatestMembers(date=self.date)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            groups = itertools.groupby(handle.iter_tsv(cols=('archive', 'member')), lambda row: row.archive)
            for archive, items in groups:
                for chunk in nwise(items, n=self.batch):
                    margs = " ".join(["'%s'" % item.member.replace('[', r'\[').replace(']', r'\]') for item in chunk])
                    shellout("""unzip -qq -c {archive} {members} |
                                sed -e 's@<?xml version="1.0" encoding="UTF-8"?>@@g' >> {output}""",
                                archive=archive, members=margs, output=stopover)

        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'), format=TSV)

class JstorIntermediateSchema(JstorTask):
    """ Convert to intermediate format via span. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
       return {'span': Executable(name='span-import', message='http://git.io/vI8NV'),
               'file': JstorXML(date=self.date)}

    @timed
    def run(self):
        output = shellout("span-import -i jstor {input} > {output}", input=self.input().get('file').path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class JstorISSNList(JstorTask):
    """ A list of JSTOR ISSNs. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return JstorIntermediateSchema(date=self.date)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout("""jq -r '.["rft.issn"][]' {input} 2> /dev/null >> {output} """, input=self.input().path, output=stopover)
        shellout("""jq -r '.["rft.eissn"][]' {input} 2> /dev/null >> {output} """, input=self.input().path, output=stopover)
        output = shellout("""sort -u {input} > {output} """, input=stopover)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class JstorDOIList(JstorTask):
    """ A list of JSTOR DOIs. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return JstorIntermediateSchema(date=self.date)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout("""jq -r '.doi' {input} | grep -v null >> {output} """, input=self.input().path, output=stopover)
        output = shellout("""sort -u {input} > {output} """, input=stopover)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
