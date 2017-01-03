# coding: utf-8
# pylint: disable=C0301

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
Common tasks.
"""

import hashlib
import os
import pipes

import luigi

from gluish.common import Executable
from gluish.format import TSV
from gluish.utils import shellout
from siskin.task import DefaultTask
from siskin.utils import iterfiles, random_string


class CommonTask(DefaultTask):
    """
    A base class for common classes. These artefacts will be written to the
    systems tempdir.
    """
    TAG = 'common'


class Directory(luigi.Task):
    """ Create directory or fail. """
    path = luigi.Parameter(description='directory to create')

    def run(self):
        try:
            os.makedirs(self.path)
        except OSError as err:
            if err.errno == 17:
                # file exists, this can happen in parallel execution evns
                pass
            else:
                raise RuntimeError(err)

    def output(self):
        return luigi.LocalTarget(self.path)


class FTPMirror(CommonTask):
    """
    A generic FTP directory sync. Required lftp (http://lftp.yar.ru/).
    The output of this task is a single file, that contains the paths
    to all the mirrored files.
    """
    host = luigi.Parameter()
    username = luigi.Parameter(default='anonymous')
    password = luigi.Parameter(default='')
    pattern = luigi.Parameter(default='*', description="e.g. '*leip_*.zip'")
    base = luigi.Parameter(default='.')
    indicator = luigi.Parameter(default=random_string())
    max_retries = luigi.IntParameter(default=5, significant=False)
    timeout = luigi.IntParameter(
        default=10, significant=False, description='timeout in seconds')
    exclude_glob = luigi.Parameter(
        default="", significant=False, description='globs to exclude')

    def requires(self):
        return Executable(name='lftp', message='http://lftp.yar.ru/')

    def run(self):
        """ The indicator is always recreated, while the subdir
        for a given (host, username, base, pattern) is just synced. """
        base = os.path.dirname(self.output().path)
        subdir = hashlib.sha1('{host}:{username}:{base}:{pattern}'.format(
            host=self.host, username=self.username, base=self.base,
            pattern=self.pattern)).hexdigest()

        # target is the root of the mirror
        target = os.path.join(base, subdir)
        if not os.path.exists(target):
            os.makedirs(target)

        exclude_glob = ""
        if not self.exclude_glob == "":
            exclude_glob = "--exclude-glob %s" % self.exclude_glob

        command = """lftp -u {username},{password}
        -e "
            set sftp:auto-confirm yes;
            set net:max-retries {max_retries};
            set net:timeout {timeout};
            set mirror:parallel-directories 1;
            set ssl:verify-certificate no;
            set ftp:ssl-protect-data true;

        mirror --verbose=0 --only-newer {exclude_glob} -I {pattern} {base} {target}; exit" {host}"""

        shellout(command, host=self.host, username=pipes.quote(self.username),
                 password=pipes.quote(self.password),
                 pattern=pipes.quote(self.pattern),
                 target=pipes.quote(target),
                 base=pipes.quote(self.base),
                 max_retries=self.max_retries,
                 timeout=self.timeout,
                 exclude_glob=exclude_glob)

        with self.output().open('w') as output:
            for path in iterfiles(target):
                self.logger.debug("Mirrored: %s", path)
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True), format=TSV)


class FTPFile(CommonTask):
    """ Just require a single file from an FTP server. """
    host = luigi.Parameter()
    username = luigi.Parameter()
    password = luigi.Parameter()
    filepath = luigi.Parameter()

    def requires(self):
        return Executable(name='lftp')

    def run(self):
        command = """lftp -u {username},{password}
        -e "set net:max-retries 5; set net:timeout 10; get -c
        {filepath} -o {output}; exit" {host}"""

        output = shellout(command, host=self.host,
                          username=pipes.quote(self.username),
                          password=pipes.quote(self.password),
                          filepath=pipes.quote(self.filepath))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True, ext=None))
