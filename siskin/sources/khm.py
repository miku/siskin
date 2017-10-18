# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301
# Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
#                   Tracy Hoffmann, <tracy.hoffmann@uni-leipzig.de>
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
KHM Koeln, refs #9269, #8391.

Configuration keys:

[khm]

scp-src = user@ftp.example.com:/home/gbi

"""

import datetime
import itertools
import os
import re
import tempfile

import luigi
from gluish.common import Executable
from gluish.format import TSV
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.benchmark import timed
from siskin.common import FTPMirror
from siskin.task import DefaultTask
from siskin.utils import iterfiles


class KHMTask(DefaultTask):
    """ Base task for KHM Koeln (sid 109) """
    TAG = '109'
    FILEPATTERN = r'.*aleph.ALL_RECS.(?P<date>[12][0-9]{3,3}[01][0-9][0123][0-9]).*(?P<no>[0-9]{1,}).tar.gz'

    def closest(self):
        return monthly(date=self.date)


class KHMDropbox(KHMTask):
    """
    Pull down content from FTP.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return Executable('rsync', message='https://rsync.samba.org/')

    def run(self):
        target = os.path.join(self.taskdir(), 'mirror')
        shellout("mkdir -p {target} && rsync {rsync_options} {src} {target}",
                 rsync_options=self.config.get('khm', 'rsync-options', fallback='-avzP'),
                 src=self.config.get('khm', 'scp-src'), target=target)

        if not os.path.exists(self.taskdir()):
            os.makedirs(self.taskdir())

        with self.output().open('w') as output:
            for path in sorted(iterfiles(target)):
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)


class KHMLatestDate(KHMTask):
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return KHMDropbox(date=self.date)

    def run(self):
        """
        Naming schema: aleph.ALL_RECS.20170316.123655.2.tar.gz. aleph.ALL_RECS.YYYYMMDD.HHMMSS.NO.tar.gz
        """
        with self.input().open() as handle:
            with self.output().open('w') as output:
                records = sorted(handle.iter_tsv(cols=('path',)), reverse=True)
                if len(records) == 0:
                    raise RuntimeError('no files found, cannot determine latest date')
                groups = re.search(self.FILEPATTERN, records[0].path).groupdict()
                output.write_tsv(groups["date"])

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class KHMLatest(KHMTask):
    """
    Decompress and join files from the (presumably) lastest version.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    ext = luigi.Parameter(default="xml", description="mrc, xml", significant=False)

    def requires(self):
        return {
            'dropbox': KHMDropbox(date=self.date),
            'date': KHMLatestDate(date=self.date),
        }

    def run(self):
        """
        Naming schema: aleph.ALL_RECS.20170316.123655.2.tar.gz. aleph.ALL_RECS.YYYYMMDD.HHMMSS.NO.tar.gz

        Ugly XML merge.
        """
        with self.input().get('date').open() as handle:
            latest_date = handle.read().strip()

        _, stopover = tempfile.mkstemp(prefix='siskin-')

        shellout(""" echo '<?xml version = "1.0" encoding = "UTF-8"?>
                           <collection xmlns="http://www.loc.gov/MARC21/slim"
                                       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                                       xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">
            ' >> {stopover} """, stopover=stopover, preserve_whitespace=True)

        with self.input().get('dropbox').open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                groups = re.search(self.FILEPATTERN, row.path).groupdict()
                if groups["date"] != latest_date:
                    continue
                shellout("tar xOf {input} | xmlcutty -path /OAI-PMH/ListRecords/record/metadata/record >> {stopover}", input=row.path, stopover=stopover)

        shellout(""" echo '</collection>' >> {stopover} """, stopover=stopover)
        output = shellout(""" xmllint --format {input} |
                              grep -v '<controlfield tag="LDR">' |
                              grep -v '<controlfield tag="FMT">' > {output} """, input=stopover)
        if self.ext == "mrc":
            output = shellout(""" yaz-marcdump -i marcxml -o marc {input} > {output}""", input=output)

        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext=self.ext))


class KHMIntermediateSchema(KHMTask):
    """
    Convert to intermediate schema via metafacture.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return KHMLatest(date=self.date)

    def run(self):
        mapdir = 'file:///%s' % self.assets("maps/")
        output = shellout("""flux.sh {flux} in={input} MAP_DIR={mapdir} > {output}""",
                          flux=self.assets("109/109.flux"), mapdir=mapdir, input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))
