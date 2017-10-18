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
NRW music tasks. Deprecated, refs #9014.

Config
------

[nrw]

url56 = url_56
url57 = url_57
url58 = url_58

"""

import datetime
import email.utils as eut

import luigi
import requests
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.task import DefaultTask


class NRWTask(DefaultTask):
    """ Base task for NRW music (sid 56,57,58) """
    TAG = 'nrw'

    def closest(self):
        """
        Use the HTTP Last-Modified header to determine the most recent date.

        If we cannot determine the date, we fail (maybe fallback to some weekly value instead).
        """
        if not hasattr(self, 'sid'):
            raise AttributeError('assumed task has a parameter sid, but it does not')

        url = self.config.get('nrw', 'url%s' % self.sid)

        resp = requests.head(url)
        if resp.status_code != 200:
            raise RuntimeError('%s on %s' % (resp.status_code, self.url))

        value = resp.headers.get('Last-Modified')
        if value is None:
            raise RuntimeError('HTTP Last-Modified header not found')

        parsed_date = eut.parsedate(value)
        if parsed_date is None:
            raise RuntimeError('could not parse Last-Modifier header')

        last_modified_date = datetime.date(*parsed_date[:3])

        return last_modified_date


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
        return luigi.LocalTarget(path=self.path(ext='fincmarc.xml'))
