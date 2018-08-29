# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301

# Copyright 2018 by Leipzig University Library, http://ub.uni-leipzig.de
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
Refs #13842.

Mediathekviewweb (https://mediathekviewweb.de/) is a German media archival
project.  Its archive can currently be found at
https://verteiler.mediathekviewweb.de/archiv/.

Daily file file exports, xz.

    $ xz -l 2018-08-28-filme.xz 
    Strms  Blocks   Compressed Uncompressed  Ratio  Check   Filename
        1       1     23.8 MiB    147.2 MiB  0.162  CRC64   2018-08-28-filme.xz

The file 2018-08-28-filme can be parsed as JSON, but it is not JSON (cf.
https://git.io/fATFM, https://git.io/fATNI).

The first archive entry dates back to 2015-03-08,
https://verteiler.mediathekviewweb.de/archiv/2015/03/2015-03-08-filme.xz.

Config
------

[mediathekviewweb]

archive = https://verteiler.mediathekviewweb.de/archiv

"""

import datetime
import luigi
from gluish.utils import shellout
from siskin.task import DefaultTask


def yesterday():
    """
    Refs: #13842, #note-30.
    """
    return datetime.date.today() - datetime.timedelta(1)

class MVWTask(DefaultTask):
    """
    Media view web.
    """
    TAG = '169'


class MVWDownload(MVWTask):
    """
    Download file list for a given date.
    """
    date = luigi.DateParameter(default=yesterday())

    def run(self):
        url = "%s/%04d/%02d/%s-filme.xz" % (self.config.get("mediathekviewweb", "archive"),
                                        self.date.year, self.date.month, self.date)
        output = shellout("""curl --fail "{url}" > {output}""", url=url)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="xz"))


class MVWMARC(MVWTask):
    """
    Convert to a MARC via script.

    Note: The MARC file for today will request the download for yesterday.

        $ taskdeps MVWMARC
          └─ MVWMARC(date=2018-08-29)
                └─ MVWDownload(date=2018-08-28)

    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return MVWDownload(date=self.date - datetime.timedelta(1))

    def run(self):
        output = shellout("python {script} <(xz -dc {input}) {output}",
                          script=self.assets("169/169_marcbinary.py"),
                          input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="fincmarc.mrc"))

