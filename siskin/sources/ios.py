# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301

# Copyright 2023 by Leipzig University Library, http://ub.uni-leipzig.de
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
IOS, refs #24731, backlog should be a directory containing one or more plain
XML files; delivery TBD

Config

[ios]

filename = prod_BYDbJQ_01_ios_journals_2023-2024_20240102.zip
share_id = 1234
share_pw = 1234
backlog = /path/to/ios/
"""

import datetime

import luigi
from gluish.format import Zstd
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.task import DefaultTask


class IOSTask(DefaultTask):
    """
    Base task for IOS Press, refs #24731.
    """
    TAG = "219"

    def closest(self):
        return monthly(self.date)


class IOSSync(IOSTask):
    """
    Sync from owncloud.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout(
            """
            curl -su "{share_id}:{share_pw}" "https://owncloud.gwdg.de/public.php/webdav/{filename}" -o {output}
            """,
            filename=self.config.get("ios", "filename"),  # e.g. prod_BYDbJQ_01_ios_journals_2023-2024_20240102.zip
            share_id=self.config.get("ios", "share_id"),
            share_pw=self.config.get("ios", "share_pw"))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="zip"))


class IOSBacklogIntermediateSchema(IOSTask):
    """
    Convert backlog to intermediate schema.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("""
                          find {backlog} -type f -name '*.xml' |
                          xargs -I {{}} cat {{}} |
                          span-import -i ios |
                          zstd -c -T0 > {output}
                          """,
                          backlog=self.config.get("ios", "backlog"))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="ndj.zst"), format=Zstd)


class IOSIntermediateSchema(IOSTask):
    """
    Convert to intermediate schema, requires span 0.1.356 or higher; combine,
    roughly deduplicate.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            "sync": IOSSync(date=self.date),
            "backlog": IOSBacklogIntermediateSchema(date=self.date),
        }

    def run(self):
        output = shellout("""
                          unzip -p {input} '*.xml' |
                          span-import -i ios |
                          zstd -c -T0 > {output} &&
                          cat {backlog} >> {output} &&
                          zstdcat -T0 {output} | LC_ALL=C sort -S10% -u | zstd -c -T0 | sponge {output}
                          """,
                          input=self.input().get("sync").path,
                          backlog=self.input().get("backlog").path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="ndj.zst"), format=Zstd)
