# coding: utf-8
# pylint: disable=W0232,E1101,R0904,E1103,C0301

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
B3Kat, #8697.
"""

import datetime
import os
import tempfile

import luigi
from gluish.format import TSV
from gluish.intervals import semiyearly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.task import DefaultTask


class B3KatTask(DefaultTask):
    """ Base task for B3Kat. """
    TAG = 'b3kat'

    def closest(self):
        """ Find date on this site: https://www.bib-bvb.de/web/b3kat/open-data """
        return semiyearly(date=self.date)


class B3KatLinks(B3KatTask):
    """
    A list of latest links. Currently grepped from https://www.bib-bvb.de/web/b3kat/open-data.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("""
            curl --fail -s "https://www.bib-bvb.de/web/b3kat/open-data" |
            grep -Eo "/OpenData/b3kat_export_[0-9]{{4,4}}_[0-9]{{1,2}}_teil[0-9]{{1,2}}.xml.gz" |
            awk '{{print "https://www.bib-bvb.de"$0 }}' > {output} """)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class B3KatDownload(B3KatTask):
    """
    Download snapshot. Output is a single (large) MARC binary file. Strip
    non-sorting characters (U+0098, U+009C) from raw data. XXX: Single core.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return B3KatLinks(date=self.date)

    def run(self):
        """
        XXX: wrapped sed ineffective.
        """
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for i, row in enumerate(handle.iter_tsv(cols=('url',)), start=1):
                downloaded = shellout("""curl -sL --fail "{url}" > {output} """, url=row.url)
                cleaned = shellout(r"""CHARS=$(python -c 'print u"\u0098\u009C".encode("utf8")');
                                       unpigz -c {input} | sed 's/['"$CHARS"']//g' > {output} """, input=downloaded)
                output = shellout("""yaz-marcdump -i marcxml -o marc "{input}" > {output}""", input=cleaned)
                shellout("cat {input} >> {stopover}",
                         input=output, stopover=stopover)
                try:
                    os.remove(downloaded)
                    os.remove(cleaned)
                    os.remove(output)
                except OSError as err:
                    self.logger.error(err)

        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))


class B3KatMARCXML(B3KatTask):
    """
    Convert binary MARC to MARCXML.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return B3KatDownload(date=self.date)

    def run(self):
        output = shellout("yaz-marcdump -i marc -o marcxml {input} | pigz -c > {output}",
                          input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml.gz'))
