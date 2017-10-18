# coding: utf-8
# pylint: disable=C0301,E1101

# Copyright 2016 by Leipzig University Library, http://ub.uni-leipzig.de
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
Gutenberg
---------

https://www.gutenberg.org/, refs #10875, #5520.

"""

import datetime

import luigi
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.task import DefaultTask


class GutenbergTask(DefaultTask):
    """
    Base task for Gutenberg.
    """
    TAG = '1'

    def closest(self):
        return weekly(date=self.date)


class GutenbergDownload(GutenbergTask):
    """
    Download RDF dump.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("wget -O {output} http://www.gutenberg.org/cache/epub/feeds/rdf-files.tar.zip")
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='tar.zip'))


class GutenbergMARC(GutenbergTask):
    """
    Extract and convert.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GutenbergDownload(date=self.date)

    def run(self):
        output = shellout("unzip -p {input} | tar -xO | python {script} - > {output}",
                          input=self.input().path, script=self.assets('1/1_marcbinary.py'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='fincmarc.mrc'))
