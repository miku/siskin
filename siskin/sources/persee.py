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
Persee, refs #3133, #11349.

# Config

[persee]

# one issn per line
fid-issn-url = http://example.com/fid-issn-list
"""

import datetime

import luigi
import requests

import marcx
import pymarc
import ujson as json
from gluish.format import TSV
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.sources.amsl import AMSLService
from siskin.task import DefaultTask


class PerseeTask(DefaultTask):
    """ Base task. """
    TAG = '39'

    def closest(self):
        return monthly(self.date)


class PerseeCombined(PerseeTask):
    """
    Harvest and combine.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        """
        Explicit from, because earliestDate might be off, refs #11349
        (#note-65). In doubt, delete the cache via `rm -rf $(metha-sync -dir
        http://oai.persee.fr/oai)`.
        """
        output = shellout("""
            METHA_DIR={metha_dir} metha-sync -format marc -from 2000-01-01 http://oai.persee.fr/oai &&
            METHA_DIR={metha_dir} metha-cat -format marc http://oai.persee.fr/oai | pigz -c > {output}
        """, metha_dir=self.config.get('core', 'metha-dir'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml.gz'))


class PerseeMARC(PerseeTask):
    """
    Custom convert to MARC.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return PerseeCombined(date=self.date)

    def run(self):
        issnfile = shellout(""" curl -sL --fail "{fid_issn_url}" > {output} """,
                            fid_issn_url=self.config.get('persee', 'fid-issn-url'))
        output = shellout("""python {script} <(unpigz -c {input}) {output} {issnfile}""",
                          script=self.assets('39/39_marcbinary.py'),
                          input=self.input().path,
                          issnfile=issnfile)
        luigi.LocalTarget(output).move(self.output().path)
        os.remove(issnfile)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='fincmarc.mrc'))

    def on_success(self):
        """
        Signal for manual update. This is decoupled from the task output,
        because it might be deleted, when imported.
        """
        self.output().copy(self.path(ext='fincmarc.mrc.import'))
