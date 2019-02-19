# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301

# Copyright 2019 by Leipzig University Library, http://ub.uni-leipzig.de
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
VKFilm Duesseldorf, #8392, MAB shipments.

External Java app required (https://sourceforge.net/projects/dnb-conv-tools/):

    $ sha1sum Mab2Mabxml-1.9.9.jar
    b37c4663fe6e4dcc55e71253266516e417ec9c44  Mab2Mabxml-1.9.9.jar

Config
------

[vkfilmdus]

input = /path/to/file.zip
Mab2Mabxml.jar = /path/to/local/copy.jar

"""

import datetime
import os

import luigi

from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask


class VKFilmDusTask(DefaultTask):
    """
    Base task for VK FILM DUESSELDORF, refs #8392.
    """
    TAG = '142'

    def closest(self):
        """
        Cf. 8392#note-38, sha1sum 55b5e6c026c5f07eeaafd3a8f04e14d79e63f20b.
        """
        return datetime.date(2019, 2, 19)


class VKFilmDusConvert(VKFilmDusTask):
    """
    Convert from binary MAB to MABXML.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        """
        Extract zip on the fly, convert with Mab2Mabxml.jar.
        """
        mabfile = shellout("unzip -p {input} > {output}",
                          input=self.config.get('vkfilmdus', 'input'), encoding='utf-8')
        output = shellout("java -jar {jarfile} -encin ISO-8859-1 -i {input} -o {output}",
                          jarfile=self.config.get("vkfilmdus", "Mab2Mabxml.jar"),
                          input=mabfile)
        luigi.LocalTarget(output).move(self.output().path)
        os.remove(mabfile)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))

