# coding: utf-8
# pylint: disable=F0401,W0232,E1101,C0103,C0301,W0223,E1123,R0904,E1103

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
"""
Open Citations, https://opencitations.net/.
"""

import tempfile

import luigi
from gluish.format import Gzip
from gluish.utils import shellout

from siskin.task import DefaultTask


class OCITask(DefaultTask):
    """
    Base task for OCI.
    """

    TAG = "oci"
    download_url = luigi.Parameter(
        default="https://figshare.com/ndownloader/articles/6741422/versions/11"
    )


class OCIDownload(OCITask):
    """
    Download via configured URL, about 30G compressed (2021).
    """

    def run(self):
        output = shellout("""curl -sL "{url}" > {output}""", url=self.download_url)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="zip", digest=True), format=Gzip)


class OCISingleFile(OCITask):
    """
    OCI as a single task.
    """

    def requires(self):
        return OCIDownload()

    def run(self):
        with tempfile.TemporaryDirectory(prefix="siskin-") as tmpdname:
            shellout("unzip -d {dir} {file}", dir=tmpdname, file=self.input().path)
            output = shellout(
                """
                              for f in $(find {dir} -name "*zip"); do
                                  unzip -p $f;
                              done | LC_ALL=C grep -vF 'oci,citing' | zstd -c -T0 > {output}
                              """,
                dir=tmpdname,
            )
            luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(
            path=self.path(ext="ndj.zst", digest=True), format=Gzip
        )


class OCICitingDOI(OCITask):
    """
    List of all citing doi.
    """

    def requires(self):
        return OCISingleFile()

    def run(self):
        output = shellout(
            """zstdcat -T0 "{input}" | cut -d , -f 2 | zstd -c -T0 > {output}
                          """,
            input=self.input().path,
        )
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(
            path=self.path(ext="ndj.zst", digest=True), format=Gzip
        )


class OCICitedDOI(OCITask):
    """
    List of all cited doi.
    """

    def requires(self):
        return OCISingleFile()

    def run(self):
        output = shellout(
            """zstdcat -T0 "{input}" | cut -d , -f 3 | zstd -c -T0 > {output}
                          """,
            input=self.input().path,
        )
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(
            path=self.path(ext="ndj.zst", digest=True), format=Gzip
        )


class OCICitingDOIUnique(OCITask):
    """
    List of all unique citing doi.
    """

    def requires(self):
        return OCICitingDOI()

    def run(self):
        output = shellout(
            """zstdcat -T0 "{input}" | LC_ALL=C sort -u -S 25% | zstd -c -T0 > {output}
                          """,
            input=self.input().path,
        )
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(
            path=self.path(ext="tsv.zst", digest=True), format=Gzip
        )


class OCICitedDOIUnique(OCITask):
    """
    List of all unique cited doi.
    """

    def requires(self):
        return OCICitedDOI()

    def run(self):
        output = shellout(
            """zstdcat -T0 "{input}" | LC_ALL=C sort -u -S 25% | zstd -c -T0 > {output}
                          """,
            input=self.input().path,
        )
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(
            path=self.path(ext="tsv.zst", digest=True), format=Gzip
        )


class OCIDOIUnique(OCITask):
    """
    Unique DOI in OCI.
    """

    def requires(self):
        return [OCICitingDOIUnique(), OCICitedDOIUnique()]

    def run(self):
        inputs = " ".join([t.path for t in self.input()])
        output = shellout(
            """zstdcat -T0 {inputs} | LC_ALL=C sort -u -S 25% | zstd -c -T0 > {output}
                              """,
            inputs=inputs,
        )
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(
            path=self.path(ext="tsv.zst", digest=True), format=Gzip
        )
