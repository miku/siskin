# coding: utf-8
# pylint: disable=C0301,E1101

# Copyright 2016 by Leipzig University Library, http://ub.uni-leipzig.de
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
BASE, refs #5994.

* https://www.base-search.net/
* https://api.base-search.net/
* https://en.wikipedia.org/wiki/BASE_(search_engine)
* http://www.dlib.org/dlib/june04/lossau/06lossau.html

Currently (Dec 2018) BASE is provided via SLUB download.

Config
------

[base]

ftp-host = sftp://example.com
ftp-base = /target_data/xyz
ftp-username= admin
ftp-password= s3cr3t
ftp-pattern = *
"""

import datetime
import os

import luigi
from gluish.format import TSV, Gzip
from gluish.utils import shellout
from siskin.common import FTPMirror
from siskin.task import DefaultTask


class BaseTask(DefaultTask):
    """
    Various tasks around base, refs #14947.
    """
    TAG = '126'


class BasePaths(BaseTask):
    """
    Mirror SLUB FTP for base.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    max_retries = luigi.IntParameter(default=10, significant=False)
    timeout = luigi.IntParameter(default=20, significant=False,
                                 description='timeout in seconds')

    def requires(self):
        return FTPMirror(host=self.config.get('base', 'ftp-host'),
                         base=self.config.get('base', 'ftp-base'),
                         username=self.config.get('base', 'ftp-username'),
                         password=self.config.get('base', 'ftp-password'),
                         pattern=self.config.get('base', 'ftp-pattern'),
                         max_retries=self.max_retries,
                         timeout=self.timeout)

    def run(self):
        self.input().move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class BaseSingleFile(BaseTask):
    """
    Create a single compressed file of tarball.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return BasePaths(date=self.date)

    def run(self):
        with self.input().open() as handle:
            paths = [p.strip().decode('utf-8') for p in handle.readlines()]

        for path in paths:
            if not path.endswith("finc/latest"):
                continue

            realpath = os.path.realpath(path)
            self.logger.debug("found: %s", realpath)

            if realpath.endswith("tar.gz"):
                output = shellout(""" tar -xOzf "{input}" | pigz -c > {output}""", input=realpath)
                luigi.LocalTarget(output).move(self.output().path)
                break
            elif realpath.endswith("gz"):
                output = shellout("""cp "{input}" "{output}" """, input=realpath)
                luigi.LocalTarget(output).move(self.output().path)
                break
            else:
                raise RuntimeError('neither tarball nor gz: %s', realpath)
        else:
            raise RuntimeError('no finc/latest in %s', paths)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ndj.gz'), format=Gzip)

