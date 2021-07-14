# coding: utf-8
# pylint: disable=C0301,E1101

# Copyright 2021 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
#                   Martin Czygan, <czygan@ub.uni-leipzig.de>
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
> The place to share your research OSF is a free, open platform to support your
research and enable collaboration.

* https://developer.osf.io/

https://api.osf.io/v2/preprints/?filter[provider]=mediarxiv&format=json&page=1

"""

import datetime

import requests

import luigi
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from siskin.task import DefaultTask


class OSFTask(DefaultTask):
    """
    https://api.osf.io/v2/preprints/?filter[provider]=mediarxiv&format=json&page=1
    """
    TAG = 'osf'

    def closest(self):
        return weekly(date=self.date)


class OSFDownload(OSFTask):
    """ Download file. """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        page = 1
        with self.output().open("w") as output:
            while True:
                link = "https://api.osf.io/v2/preprints/?page={}".format(page)
                self.logger.debug("osf: {}".format(link))
                resp = requests.get(link)
                if resp.status_code == 404:
                    break
                if resp.status_code != 200:
                    raise RuntimeError('osf api failed with {}'.format(resp.status_code))
                self.logger.debug("fetched {} from {}: {}".format(len(resp.text), link, resp.text[:40]))
                output.write(resp.text)
                output.write("\n")
                page += 1

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))