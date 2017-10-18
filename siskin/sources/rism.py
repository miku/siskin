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

"""
RISM, refs #4435.

[rism]

base-url = http://example.com/export

"""

import datetime
import os

import luigi
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.task import DefaultTask


class RISMTask(DefaultTask):
    """ Base task for RISM. """
    TAG = "14"

    def closest(self):
        return monthly(self.date)


class RISMDownload(RISMTask):
    """
    Download raw data and prepare. Assume the URL (https://is.gd/ZTwzoT) of the
    complete dump does not change.

        Archive:  /tmp/gluish-S4wdR6
        Length      Date    Time    Name
        ---------  ---------- -----   ----
        3804391409  2017-03-23 14:46   rism_170316.xml <<<< We are only interested in this for now.
        52058685    2017-03-23 14:46   rism_ks.xml
        26080795    2017-03-23 14:46   rism_lit.xml
        191009578   2017-03-23 14:46   rism_pe.xml
        ---------                     -------
        4073540467                     4 files
    """

    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        """
        Hack around the fact, that we do not know the exact filename
        (rism_170316.xml), but let's assume there is a pattern.
        """
        cleanup = set()
        url = "https://opac.rism.info/fileadmin/user_upload/lod/update/rismAllMARCXML.zip"

        output = shellout("""curl --fail "{url}" > {output} """, url=url)
        cleanup.add(output)

        output = shellout("""unzip -p {input} $(unzip -l {input} | grep -Eo "rism_[0-9]{{6,6}}.xml" | head -1) > {output}""", input=output)
        cleanup.add(output)

        output = shellout("yaz-marcdump -i marcxml -o marc {input} > {output}", input=output)
        luigi.LocalTarget(output).move(self.output().path)

        for file in cleanup:
            os.remove(file)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))


class RISMMARC(RISMTask):
    """ Transform MARC. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return RISMDownload(date=self.date)

    def run(self):
        output = shellout("""python {script} {input} {output}""",
                          script=self.assets("14/14_marcbinary.py"),
                          input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='fincmarc.mrc'))
