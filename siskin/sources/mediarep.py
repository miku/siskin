# coding: utf-8
# pylint: disable=C0301,E1101

# Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
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
mediarep.org, refs #14064.
"""

import datetime
import os

import luigi
from gluish.common import Executable
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.task import DefaultTask


class MediarepTask(DefaultTask):
    """
    Base task.
    """
    TAG = '170'

    def closest(self):
        return monthly(date=self.date)


class MediarepMARC(MediarepTask):
    """
    Harvest and convert to MARC.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="https://mediarep.org/oai/request",
                          significant=False)
    prefix = luigi.Parameter(default="oai_dc", significant=False)

    def requires(self):
        return Executable(name='metha-sync', message='https://github.com/miku/metha'),

    def run(self):
        shellout("METHA_DIR={dir} metha-sync -format {prefix} {url}",
                 prefix=self.prefix, url=self.url, dir=self.config.get('core', 'metha-dir'))
        data = shellout("""METHA_DIR={dir} metha-cat -root Records -format {prefix} {url} > {output}""", dir=self.config.get('core', 'metha-dir'), prefix=self.prefix, url=self.url)
        output = shellout("""python {script} {input} {output}""", script=self.assets('170/170_marcbinary.py'), input=data)
        luigi.LocalTarget(output).move(self.output().path)

        os.remove(data)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="fincmarc.mrc"))

class MediarepIntermediateSchema(MediarepTask):
    """
    Single file dump.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="https://mediarep.org/oai/request",
                          significant=False)
    prefix = luigi.Parameter(default="dim", significant=False)

    def requires(self):
        return [
            Executable(name='metha-sync', message='https://github.com/miku/metha'),
            Executable(name='span-import', message='https://github.com/miku/span'),
        ]

    def run(self):
        shellout("METHA_DIR={dir} metha-sync -format {prefix} {url}",
                 prefix=self.prefix, url=self.url, dir=self.config.get('core', 'metha-dir'))
        output = shellout("""METHA_DIR={dir} metha-cat -root Records -format {prefix} {url} |
                             span-import -i mediarep-dim | pigz -c > {output}""",
                          prefix=self.prefix, url=self.url, dir=self.config.get('core', 'metha-dir'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="ldj.gz"))
