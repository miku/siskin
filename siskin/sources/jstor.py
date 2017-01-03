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
[jstor]

ftp-host = host.name
ftp-username = username
ftp-password = password
ftp-path = /
ftp-pattern = *
"""

import datetime
import itertools
import pipes
import tempfile

import luigi

from gluish.format import TSV
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.benchmark import timed
from siskin.common import Executable, FTPMirror
from siskin.sources.amsl import AMSLFilterConfig
from siskin.task import DefaultTask
from siskin.utils import nwise


class JstorTask(DefaultTask):
    """ Jstor base. """
    TAG = 'jstor'

    def closest(self):
        return weekly(self.date)


class JstorPaths(JstorTask):
    """
    Sync.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    max_retries = luigi.IntParameter(default=10, significant=False)
    timeout = luigi.IntParameter(
        default=20, significant=False, description='timeout in seconds')

    def requires(self):
        return FTPMirror(host=self.config.get('jstor', 'ftp-host'),
                         username=self.config.get('jstor', 'ftp-username'),
                         password=self.config.get('jstor', 'ftp-password'),
                         pattern=self.config.get('jstor', 'ftp-pattern'),
                         max_retries=self.max_retries,
                         timeout=self.timeout)

    def run(self):
        self.input().move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class JstorMembers(JstorTask):
    """
    Extract a full list of archive members.
    TODO: This should only be done once per file.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return JstorPaths(date=self.date)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                if not row.path.endswith('.zip'):
                    self.logger.debug('skipping: %s', row.path)
                    continue
                shellout(""" unzip -l {input} | LC_ALL=C grep "xml$" | LC_ALL=C awk '{{print "{input}\t"$4}}' | LC_ALL=C sort >> {output} """,
                         preserve_whitespace=True, input=row.path, output=stopover)
        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class JstorLatestMembers(JstorTask):
    """
    Find the latest archive members for all article ids.

    The path names are like XXX_YYYY-MM-DD-HH-MM-SS/jjjj/yyyy-mm-dd/id/id.xml.

    This way, it is possible to sort the shipments by date, run `tac` and
    only keep the latest entries.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return JstorMembers(date=self.date)

    @timed
    def run(self):
        """
        Expect input to be sorted by shipment date, so tac will actually be a perfect rewind.
        """
        output = shellout("tac {input} | LC_ALL=C sort -S 35% -u -k2,2 | LC_ALL=C sort -S 35% -k1,1 > {output}",
                          input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class JstorXML(JstorTask):
    """
    Create a snapshot of the latest data.
    TODO(miku): maybe shard by journal and reduce update time.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    batch = luigi.IntParameter(default=512, significant=False)

    def requires(self):
        return JstorLatestMembers(date=self.date)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            groups = itertools.groupby(handle.iter_tsv(
                cols=('archive', 'member')), lambda row: row.archive)
            for archive, items in groups:
                for chunk in nwise(items, n=self.batch):
                    margs = " ".join(["'%s'" % item.member.replace(
                        '[', r'\[').replace(']', r'\]') for item in chunk])
                    shellout("""unzip -p {archive} {members} |
                                sed -e 's@<?xml version="1.0" encoding="UTF-8"?>@@g' | pigz -c >> {output}""",
                             archive=archive, members=margs, output=stopover)

        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml.gz'), format=TSV)


class JstorIntermediateSchema(JstorTask):
    """
    Convert to intermediate format via span.
    """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'span': Executable(name='span-import', message='http://git.io/vI8NV'),
            'file': JstorXML(date=self.date)
        }

    @timed
    def run(self):
        output = shellout("span-import -i jstor <(unpigz -c {input}) | pigz -c > {output}",
                          input=self.input().get('file').path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))


class JstorExport(JstorTask):
    """
    Tag with ISILs, then export to various formats.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='solr5vu3', description='export format')

    def requires(self):
        return {
            'file': JstorIntermediateSchema(date=self.date),
            'config': AMSLFilterConfig(date=self.date),
        }

    def run(self):
        output = shellout("span-tag -c {config} <(unpigz -c {input}) | pigz -c > {output}",
                          config=self.input().get('config').path, input=self.input().get('file').path)
        output = shellout(
            "span-export -o {format} <(unpigz -c {input}) | pigz -c > {output}", format=self.format, input=output)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        extensions = {
            'solr5vu3': 'ldj.gz',
            'formeta': 'form.gz',
        }
        return luigi.LocalTarget(path=self.path(ext=extensions.get(self.format, 'gz')))


class JstorISSNList(JstorTask):
    """
    A list of JSTOR ISSNs.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return JstorIntermediateSchema(date=self.date)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout(
            """jq -r '.["rft.issn"][]?' <(unpigz -c {input}) 2> /dev/null >> {output} """, input=self.input().path, output=stopover)
        shellout(
            """jq -r '.["rft.eissn"][]?' <(unpigz -c {input}) 2> /dev/null >> {output} """, input=self.input().path, output=stopover)
        output = shellout("""sort -u {input} > {output} """, input=stopover)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class JstorDOIList(JstorTask):
    """
    A list of JSTOR DOIs.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return JstorIntermediateSchema(date=self.date)

    @timed
    def run(self):
        output = shellout("""jq -r '.doi' <(unpigz -c {input}) | grep -v null > {output} """,
                          input=self.input().path)
        output = shellout("""sort -u {input} > {output} """, input=output)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
