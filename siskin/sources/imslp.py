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
IMSLP, refs #1240.

Config
------

[imslp]

url = http://example.com/imslpOut_2016-12-25.tar.gz
"""

import datetime
import shutil
import tempfile

import luigi
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.task import DefaultTask


class IMSLPTask(DefaultTask):
    """ Base task for IMSLP. """
    TAG = "15"

    def closest(self):
        return datetime.date(2017, 12, 25)


class IMSLPDownload(IMSLPTask):
    """ Download raw data. Should be a single URL pointing to a tar.gz. """

    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("""wget -O {output} {url}""", url=self.config.get('imslp', 'url'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='tar.gz'))


class IMSLPConvert(IMSLPTask):
    """ Extract and transform. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return IMSLPDownload(date=self.date)

    def run(self):
        tempdir = tempfile.mkdtemp(prefix='siskin-')
        shellout("tar -xzf {archive} -C {tempdir}", archive=self.input().path, tempdir=tempdir)
        output = shellout("python {script} {tempdir} {output}",
                          script=self.assets('15/15_marcbinary.py'), tempdir=tempdir)
        shutil.rmtree(tempdir)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='fincmarc.mrc'))
