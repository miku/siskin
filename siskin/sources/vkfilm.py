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
VKFilm (Postdam), #8575, about 120000 records.

TODO: Unify VK* tasks.

Config
------

[vkfilm]

file = /path/to/file
"""

import luigi
from gluish.utils import shellout

from siskin.task import DefaultTask


class VKFilmTask(DefaultTask):
    TAG = '127'


class VKFilmFile(VKFilmTask, luigi.ExternalTask):
    """
    Manually downloaded MAB dump, #8575#note-7.
    """

    def output(self):
        return luigi.LocalTarget(path=self.config.get('vkfilm', 'file'))


class VKFilmMarc(VKFilmTask):
    """
    Convert to some MARC XML via metafacture. Custom morphs and flux are
    kept in assets/127.
    """

    def requires(self):
        return VKFilmFile()

    def run(self):
        output = shellout("""iconv -f utf-8 -t utf-8 -c {input} > {output}""", input=self.input().path)
        output = shellout("""flux.sh {flux} in={input} > {output}""",
                          flux=self.assets("127/127.flux"), input=output)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='fincmarc.xml'))
