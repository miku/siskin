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
VKFilm, #8571. Hebis FTP.

TODO: Unify VK* tasks.

Config
------

[vkfilmff]

ftp-host = host.name
ftp-username = username
ftp-password = password
ftp-path = /
ftp-pattern = *
"""

import datetime

import luigi
from gluish.format import TSV
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.common import FTPMirror
from siskin.task import DefaultTask


class VKFilmFFTask(DefaultTask):
    TAG = '119'

    def closest(self):
        return datetime.date(2016, 9, 26)


class VKFilmFFPaths(VKFilmFFTask):
    """
    Sync.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    max_retries = luigi.IntParameter(default=10, significant=False)
    timeout = luigi.IntParameter(default=20,
                                 significant=False,
                                 description='timeout in seconds')

    def requires(self):
        return FTPMirror(host=self.config.get('vkfilmff', 'ftp-host'),
                         username=self.config.get('vkfilmff', 'ftp-username'),
                         password=self.config.get('vkfilmff', 'ftp-password'),
                         pattern=self.config.get('vkfilmff', 'ftp-pattern'),
                         max_retries=self.max_retries,
                         timeout=self.timeout)

    def run(self):
        self.input().move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class VKFilmFFFincMarc(VKFilmFFTask):
    """
    Find MARC XML, uncompress, clean, remove "Nichtsortierzeichen" on the fly,
    convert via flux (refs #8571).
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return VKFilmFFPaths(date=self.date)

    def run(self):
        with self.input().open() as handle:
            filename = 'film_theater_marc_%s.xml.gz' % (self.closest().strftime("%Y%m%d"))
            for row in handle.iter_tsv(cols=('path',)):

                if not row.path.endswith(filename):
                    continue
                output = shellout("unpigz -c {file} | sed $'s/\u0098//g;s/\u009C//g' > {output}", file=row.path)
                output = shellout("yaz-marcdump -i marcxml -o marc {input} > {output}", input=output)
                output = shellout("flux.sh {flux} inputfile={input} outputfile=stdout > {output}",
                                  flux=self.assets("119/119.flux"), input=output)
                luigi.LocalTarget(output).move(self.output().path)
                break
            else:
                raise RuntimeError('not found: %s' % filename)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))
