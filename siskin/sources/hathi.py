# coding: utf-8
# pylint: disable=C0301,E1101

# Copyright 2019 by Leipzig University Library, http://ub.uni-leipzig.de
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
Hath trust, OAI, maintained by UMich
https://quod.lib.umich.edu/cgi/o/oai/oai

Source: 35
Ticket: 15487
"""

import datetime

import luigi
from gluish.common import Executable
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask


class HathiTask(DefaultTask):
    """
    Base task.
    """
    TAG = '35'

    def closest(self):
        return weekly(date=self.date)


class HathiCombine(HathiTask):
    """
    Single file dump.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="https://quod.lib.umich.edu/cgi/o/oai/oai", significant=False)
    prefix = luigi.Parameter(default="marc21")

    def requires(self):
        return Executable(name='metha-sync', message='https://github.com/miku/metha')

    def run(self):
        shellout("METHA_DIR={dir} metha-sync -format {prefix} {url}",
                 prefix=self.prefix,
                 url=self.url,
                 dir=self.config.get('core', 'metha-dir'))
        output = shellout("METHA_DIR={dir} metha-cat -root Records -format {prefix} {url} | pigz -c > {output}",
                          prefix=self.prefix,
                          url=self.url,
                          dir=self.config.get('core', 'metha-dir'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="xml.gz"))

class HathiMARC(HathiTask):
    """
    Convert to MARC.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return HathiCombine(date=self.date, prefix="marc21")

    def run(self):
        output = shellout(""" {python} {script} <(unpigz -c "{input}") {output} """,
                          python=self.config.get("core", "python"),
                          script=self.assets("35/35_marcbinary.py"),
                          input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="mrc"))
