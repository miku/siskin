# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301

# Copyright 2022 by Leipzig University Library, http://ub.uni-leipzig.de
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
Eastview.

* refs. #12586

Config
------

[eastview]

ftp-host = host.name
ftp-username = username
ftp-password = password
ftp-path = /
ftp-pattern = *

"""

import datetime
import json
import os
import tempfile
import zipfile

import luigi
from gluish.common import Executable
from gluish.format import TSV, Zstd
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.benchmark import timed
from siskin.common import FTPMirror
from siskin.conversions import eastview_solr_to_intermediate_schema
from siskin.task import DefaultTask


class EastViewTask(DefaultTask):
    TAG = 'eastview'

    def closest(self):
        return weekly(date=self.date)


class EastViewPaths(EastViewTask):
    """
    A list of IEEE file paths (via FTP).

    managed-schema
    xmlt.xsl
    xml.zip

    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        host = self.config.get('eastview', 'ftp-host')
        username = self.config.get('eastview', 'ftp-username')
        password = self.config.get('eastview', 'ftp-password')
        base = self.config.get('eastview', 'ftp-path')
        pattern = self.config.get('eastview', 'ftp-pattern')
        return FTPMirror(host=host, username=username, password=password, base=base, pattern=pattern)

    @timed
    def run(self):
        output = shellout('sort {input} > {output}', input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="filelist"), format=TSV)


class EastViewIntermediateSchema(EastViewTask):
    """
    Convert to a single (is) file.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return EastViewPaths(date=self.date)

    def run(self):
        with tempfile.NamedTemporaryFile(delete=False, mode="w") as tf:
            zname = None
            with self.input().open(mode='r') as f:
                for line in f:
                    line = line.decode("utf-8").strip()
                    if not line.endswith("xml.zip"):
                        continue
                    zname = line
                    break
            if zname is None:
                raise ValueError("expected a file named xml.zip")
            with zipfile.ZipFile(zname) as zf:
                for name in zf.namelist():
                    if not name.endswith(".xml"):
                        continue
                    with zf.open(name) as f:
                        docs = eastview_solr_to_intermediate_schema(f.read())
                    self.logger.debug("{} {}".format(name, len(docs)))
                    for doc in docs:
                        tf.write(json.dumps(doc) + "\n")
        output = shellout("zstd -c -T0 < {tf} > {output}", tf=tf.name)
        luigi.LocalTarget(output).move(self.output().path)
        os.remove(tf.name)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="json.zst"), format=Zstd)
