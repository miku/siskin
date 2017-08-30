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
import tempfile

import luigi
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.task import DefaultTask


class RISMTask(DefaultTask):
    """ Base task for RISM, refs. #4435. """
    TAG = '14'

    def closest(self):
        return monthly(date=self.date)


class RISMArchives(RISMTask):
    """
    Builds a list of RISM archive urls.

    Example pattern: rismExportBvb_YYYY-MM-DD.tar.gz
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("""
            curl --fail -sL "{url}" |
            grep -Eo "rismExportBvbOut_[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9].tar.gz" |
            sort | uniq |
            awk '{{ print "{url}/"$1 }}' > {output}
        """, url=self.config.get('rism', 'base-url'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())


class RISMCombineBinaryMARC(RISMTask):
    """
    Combine and convert to binary MARC.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return RISMArchives()

    def run(self):
        """
        URL -> TAR -> XML -> MARC.
        """
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue

                archive = shellout(""" wget -O "{output}" "{url}" """, url=line)
                xml = shellout("""tar -xzOf {input} > {output} """, input=archive)
                shellout(""" yaz-marcdump -i marcxml -o marc {input} >> {output} """,
                         input=xml, output=stopover)

                os.remove(archive)
                os.remove(xml)

        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))
