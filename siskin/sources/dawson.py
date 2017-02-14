# coding: utf-8
# pylint: disable=C0301,E1101

# Copyright 2017 by Leipzig University Library, http://ub.uni-leipzig.de
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
DawsonEra, #9773, SID124.

Experimental: zip, marcxml, missing ns, 0x01, illegal characters.

Configuration
-------------

[dawson]

download-url = http://exa.com/store/123

"""

import luigi

from gluish.utils import shellout
from siskin.task import DefaultTask


class DawsonTask(DefaultTask):
    """
    Base class for Dawson.
    """
    TAG = '124'


class DawsonDownload(DawsonTask):
    """
    Download.
    """

    def run(self):
        output = shellout("curl --fail -sL {url} > {output}",
                          url=self.config.get('dawson', 'download-url'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='zip'))
