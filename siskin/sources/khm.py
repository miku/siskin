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

Update usually comes in two tarballs, carrying a 8 digit date string, but file
location might differ.

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

class KHMLatest(KHMTask):
    """
    Write out a file with latest date and path to latest files.

    20180725        .../109/KHMDropbox/mirror/khmkoeln/august-2018/aleph.ALL_RECS.20180725.125558.2.tar.gz
    20180725        .../109/KHMDropbox/mirror/khmkoeln/august-2018/aleph.ALL_RECS.20180725.125255.1.tar.gz
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return KHMDropbox(date=self.date)

    def run(self):
        """
        Naming schema: aleph.ALL_RECS.20170316.123655.2.tar.gz. aleph.ALL_RECS.YYYYMMDD.HHMMSS.NO.tar.gz
        """
        with self.input().open() as handle:
            with self.output().open('w') as output:
                records = sorted(handle.iter_tsv(cols=('path',)), reverse=True,
                                 key=lambda row: os.path.basename(row.path))
                if len(records) == 0:
                    raise RuntimeError('no files found, cannot determine latest date')
                groups = re.search(self.FILEPATTERN, records[0].path).groupdict()
                latest_date = groups['date']
                for record in records:
                    gg = re.search(self.FILEPATTERN, record.path).groupdict()
                    if gg['date'] == latest_date:
                        output.write_tsv(latest_date, record.path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class KHMMARC(KHMTask):
    """
    Convert tarball to binary MARC. Note: we expect exactly two tarballs, which
    is a bit brittle at the moment.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return KHMLatest(date=self.date)

    def run(self):
        tarballs = []
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('x', 'path')):
                tarballs.append(row.path)

        if not len(tarballs) == 2:
            raise RuntimeError('limitation: we need exactly two tarballs')

        output = shellout("python {script} {t1} {t2} {output}",
                          script=self.assets("109/109_marcbinary.py"),
                          t1=tarballs[0], t2=tarballs[1])
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="fincmarc.mrc"))
