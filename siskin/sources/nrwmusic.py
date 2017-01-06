# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301
# Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
#                   Tracy Hoffmann, <tracy.hoffmann@uni-leipzig.de>
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
nrw music tasks

Config
------

[nrw]

url56 = url_56
url57 = url_57
url58 = url_58

"""

import datetime
import tempfile

import luigi

from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask


class NRWTask(DefaultTask):
    """ Base task for NRW music (sid 56,57,58) """
    TAG = 'nrw'

    def closest(self):
        return weekly(self.date)


class NRWHarvest(NRWTask):
    """
    Download dump and unzip
    """
    date = ClosestDateParameter(default=datetime.date.today())
    sid = luigi.Parameter(default='56', description='source id')

    def run(self):
        url = self.config.get('nrw', 'url%s' % self.sid)
        output = shellout("""curl --fail {url} | tar -xOz > {output}""", url=url)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))


class NRWTransformation(NRWTask):
    """
    Transform data for finc
    """
    date = ClosestDateParameter(default=datetime.date.today())
    sid = luigi.Parameter(default='56', description='source id')

    def requires(self):
        return NRWHarvest(date=self.date, sid=self.sid)

    def run(self):
        output = shellout("""flux.sh {flux} in={input} sid={sid} > {output}""",
                          flux=self.assets("56_57_58/nrw_music.flux"), input=self.input().path, sid=self.sid)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))
