# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301

# Copyright 2018 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
#                   Robert Schenk, <schenk@ub.uni-leipzig.de>
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
Music Treasures Consortium Online, refs #5798.
"""

from __future__ import print_function

import datetime
import json
import sys
import time

import luigi
import requests
from gluish.utils import shellout

from siskin.task import DefaultTask


class MTCTask(DefaultTask):
    """
    Base task for MTC.
    """
    TAG = '10'

class MTCHarvest(MTCTask):
    """
    Harvest.
    """
    max_retries = luigi.IntParameter(default=3, significant=False)

    def run(self):
        page, retry_count = 1, self.max_retries
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT x.y; rv:10.0) Gecko/20100101 Firefox/10.0'}

        with self.output().open("w") as output:
            while True:
                url = "https://www.loc.gov/collections/music-treasures-consortium/?sp=%s&fo=json" % page
                self.logger.debug(url)
                r = requests.get(url, headers=headers)
                if r.status_code >= 400:
                    if retry_count == 0:
                        raise RuntimeError("failed after %s attempts with %s: %s",
                                           self.max_retries, r.status_code, url)
                    else:
                        self.logger.debug("retrying %s, got %s", url, r.status_code)
                        retry_count -= 1
                        continue

                doc = json.loads(r.text)
                if doc.get("status") == 404:
                    self.logger.debug("404 at %s", url)
                    break
                output.write(r.text)

                page += 1
                retry_count = self.max_retries
                time.sleep(5)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='json'))

class MTCMARC(MTCTask):
    """
    Convert custom JSON.
    """
    def requires(self):
        return MTCHarvest()

    def run(self):
        output = shellout("python {script} {input} {output}",
                          script=self.assets('10/10_marcbinary.py'),
                          input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='fincmarc.mrc'))
