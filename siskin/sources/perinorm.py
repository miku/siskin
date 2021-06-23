# coding: utf-8
# pylint: disable=C0301,E1101,C0103

# Copyright 2021 by Leipzig University Library, http://ub.uni-leipzig.de
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

import datetime
import json
import re
import tarfile

import luigi
from gluish.format import TSV, Gzip
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.common import FTPMirror
from siskin.task import DefaultTask


class PerinormTask(DefaultTask):
    """
    Base task for Perinorm, refs #16140.
    """
    TAG = '201'
    current = {
        "date": datetime.date(2021, 4, 15),
        "filename": "basic/perinorm_2021-04-15T11:35:02Z.tar.gz",
    }


class PerinormPaths(PerinormTask):
    """
    Mirror SLUB FTP.
    """
    date = luigi.DateParameter(default=PerinormTask.current["date"])
    max_retries = luigi.IntParameter(default=10, significant=False)
    timeout = luigi.IntParameter(default=20, significant=False, description='timeout in seconds')

    def requires(self):
        return FTPMirror(host=self.config.get('perinorm', 'ftp-host'),
                         base=self.config.get('perinorm', 'ftp-base'),
                         username=self.config.get('perinorm', 'ftp-username'),
                         password=self.config.get('perinorm', 'ftp-password'),
                         pattern=self.config.get('perinorm', 'ftp-pattern'),
                         max_retries=self.max_retries,
                         timeout=self.timeout)

    def run(self):
        self.input().move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())


class PerinormExport(PerinormTask):
    """
    Input file is not intermediate schema, so we cannot use span-tag currently;
    tag manually.
    """

    date = luigi.DateParameter(default=PerinormTask.current["date"])

    def requires(self):
        return PerinormPaths(date=self.date)

    def run(self):
        with self.input().open() as file:
            for line in file:
                line = line.strip()
                if not self.current["filename"] in line:
                    continue
                path = line
                break
            else:
                raise RuntimeError("could not file {} in ftp filelist".format(self.current["filename"]))
        self.logger.debug("using {}".format(path))

        # XXX: get these from amsl via: span-amsl-discovery -f -live
        # https://example.amsl.technology | grep "perinorm"
        attachments = {
            "Perinorm (DIN-Normen)": ["DE-Gla1", "DE-Zi4"],
            "Perinorm (DWA-Regelwerk)": ["DE-Gla1"],
            "Perinorm (ISO Standards)": ["DE-Gla1"],
            "Perinorm (VDI-Richtlinien)": ["DE-Gla1", "DE-Zi4"],
        }

        with self.output().open("wb") as output:
            tar = tarfile.open(path, "r:gz")
            for member in tar.getmembers():
                f = tar.extractfile(member)
                for line in f:
                    doc = json.loads(line)
                    for k, isils in attachments.items():
                        if doc["mega_collection"][0] == k:
                            doc["institution"] = isils
                    output.write(json.dumps(doc).encode("utf-8"))
                    output.write(b"\n")

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="ndj.gz"), format=Gzip)
