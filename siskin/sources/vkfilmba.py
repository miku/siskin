# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301

# Copyright 2017 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
#                   Robert Schenk, <robert.schenk@uni-leipzig.de>
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
#

"""
VKFilm Bundesarchiv, #8695.

TODO: Unify VKFilm* tasks.

Config
------

[vkfilmba]

baseurl = https://example.com

data = http://example.com/data.zip
deletions = http://example.com/del.txt

"""

import datetime
import hashlib
import os
import tempfile

import luigi

from gluish.format import TSV
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.common import FTPMirror
from siskin.task import DefaultTask


class VKFilmBATask(DefaultTask):
    TAG = '148'

    # As this is a manual process, adjust this list by hand and rerun tasks.
    filenames = [
        "adlr.zip",
        "adlr_171011.zip",
        "adlr_1712.zip",
    ]

    def fingerprint(self):
        """
        Generate a name based on the inputs, so everytime we adjust the file
        list, we get another output.
        """
        h = hashlib.sha1()
        h.update("".join(self.filenames).encode("utf-8"))
        return h.hexdigest()


class VKFilmBADownload(VKFilmBATask):
    """
    Download raw data daily. At least try, since as of 2018-02-15, file seems
    missing, refs #12460.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("""curl --fail "{url}" > {output} """,
                          url=self.config.get('vkfilmba', 'data'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        """
        We expect zip, but to guarantee.
        """
        return luigi.LocalTarget(path=self.path(ext="zip"))


class VKFilmBADownloadDeletions(VKFilmBATask):
    """
    Download raw deletions daily. At least try, since as of 2018-02-15, file seems
    missing, refs #12460.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("""curl --fail "{url}" > {output} """,
                          url=self.config.get('vkfilmba', 'deletions'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        """
        We expect txt, but to guarantee.
        """
        return luigi.LocalTarget(path=self.path(ext="txt"))


class VKFilmBADump(VKFilmBATask):
    """
    Concatenate a list of URLs.

    Note: DO NOT DELETE the output of this task. There are copies:

    * https://goo.gl/FvhXnL
    * https://speicherwolke.uni-leipzig.de/index.php/s/KMUldvGMJRc7iLP

    XXX: There is a "delete-list" of ID, which should be filtered here.
    """

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for fn in self.filenames:
            link = os.path.join(self.config.get("vkfilmba", "baseurl"), fn)
            output = shellout("""wget -O {output} "{link}" """, link=link)
            output = shellout(""" unzip -p "{input}" > "{output}" """,
                              input=output)
            output = shellout("""yaz-marcdump -i marcxml -o marc "{input}" > "{output}"  """,
                              input=output, ignoremap={5: "Fixme."})
            shellout("""cat "{input}" >> "{stopover}" """,
                     input=output, stopover=stopover)
        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(filename="%s.mrc" % self.fingerprint()))


class VKFilmBAConvert(VKFilmBATask):
    """
    Convert download, refs #12460.
    """

    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'dump': VKFilmBADump(),
            'download': VKFilmBADownload(),
        }

    def run(self):
        # XXX: workaround; we want a single file for further processing.
        filenames = [
            'adlr_1m1',
            'adlr_1801',
            'adlr',
        ]
        _, stopover = tempfile.mkstemp(prefix='siskin-')

        for fn in filenames:
            output = shellout(""" unzip -p "{input}" {fn} > "{output}" """, input=self.input().get('download').path, fn=fn)
            output = shellout("""yaz-marcdump -i marcxml -o marc "{input}" > "{output}"  """,
                              input=output, ignoremap={5: "Fixme."})
            shellout("""cat "{input}" >> "{stopover}" """,
                     input=output, stopover=stopover)

        # Concatenate, since download was only 150k, while dump was 9M?
        output = shellout("cat {a} {b} > {output}", a=self.input().get('dump').path, b=stopover)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(filename="%s.mrc" % self.fingerprint()))


class VKFilmBAMARC(VKFilmBATask):
    """
    Run conversion script.
    """

    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return VKFilmBAConvert()  # return VKFilmBADump()

    def run(self):
        output = shellout("python {script} {input} {output}",
                          script=self.assets("148/148_marcbinary.py"),
                          input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='fincmarc.mrc'))
