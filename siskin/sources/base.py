# coding: utf-8
# pylint: disable=C0301,E1101

# Copyright 2016 by Leipzig University Library, http://ub.uni-leipzig.de
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
BASE, refs #5994.

* https://www.base-search.net/
* https://api.base-search.net/
* https://en.wikipedia.org/wiki/BASE_(search_engine)
* http://www.dlib.org/dlib/june04/lossau/06lossau.html

Config
------

[base]

username = nana
password = s3cret
url = http://export.com/intermediate.file.gz

"""

import datetime

import luigi
from gluish.format import Gzip
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.task import DefaultTask


class BaseTask(DefaultTask):
    """
    Various tasks around base.
    """
    TAG = 'base'

    def closest(self):
        return weekly(self.date)


class BaseDownload(BaseTask):
    """
    Attempt to download file from a configured URL.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout(""" curl --fail -v -u {username}:{password} "{url}" > {output} """,
                          username=self.config.get('base', 'username'),
                          password=self.config.get('base', 'password'),
                          url=self.config.get('base', 'url'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)
