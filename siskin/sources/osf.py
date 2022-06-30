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

Refs: #20238.
"""

import datetime
import json
import time

import requests

import luigi
from gluish.format import Zstd
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.conversions import osf_to_intermediate
from siskin.sources.amsl import AMSLFilterConfigFreeze
from siskin.task import DefaultTask


class OSFTask(DefaultTask):
    """
    https://api.osf.io/v2/preprints/?filter[provider]=mediarxiv&format=json&page=1
    """

    TAG = "191"

    def closest(self):
        return weekly(date=self.date)


class OSFDownload(OSFTask):
    """
    Download OSF metadata via API. API is kind of slow and flaky.

    Pagination docs: https://jsonapi.org/format/#fetching-pagination

    Retrieval of full set takes: 117m13.266s - about 325M, 86651 docs.
    """

    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        page = 1
        max_retries = 20  # a "global" retry budget
        sleep_s = 60
        with self.output().open("w") as output:
            while True:
                link = "https://api.osf.io/v2/preprints/?page={}&page[size]=100".format(
                    page
                )
                self.logger.debug("osf: {}".format(link))
                resp = requests.get(link)
                if resp.status_code == 404:
                    break
                if resp.status_code != 200:
                    if max_retries > 0:
                        time.sleep(sleep_s)
                        max_retries -= 1
                        continue
                    else:
                        raise RuntimeError(
                            "osf api failed with {}".format(resp.status_code)
                        )
                self.logger.debug(
                    "fetched {} from {}: {}".format(
                        len(resp.text), link, resp.text[:40]
                    )
                )
                output.write(resp.text)
                output.write("\n")
                page += 1

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="json"))


class OSFIntermediateSchema(OSFTask):
    """
    Convert to intermediate schema (stub).
    """

    date = ClosestDateParameter(default=datetime.date.today())
    max_retries = luigi.IntParameter(
        default=5, description="number of HTTP request retries", significant=False
    )
    encoding = luigi.Parameter(default="utf-8", significant=False)

    def requires(self):
        return OSFDownload()

    def run(self):
        i = 0
        bNL = "\n".encode(self.encoding)
        with self.output().open("w") as output:
            with self.input().open() as f:
                for line in f:
                    resp = json.loads(line)
                    for doc in resp["data"]:
                        result = osf_to_intermediate(doc, max_retries=self.max_retries)
                        if i % 1000 == 0:
                            self.logger.debug("converted {} docs".format(i))
                        output.write(json.dumps(result).encode(self.encoding))
                        output.write(bNL)
                        i += 1

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="json.zst"), format=Zstd)


class OSFExport(OSFTask):
    """
    OSF should be a daily/weekly updated source, so we generate a solr
    importable file directly.
    """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            "data": OSFIntermediateSchema(),
            "config": AMSLFilterConfigFreeze(),
        }

    def run(self):
        output = shellout(
            """
            unpigz -c {input} |
            span-tag -unfreeze {config} |
            span-export |
            zstd -c -T0 > {output}""",
            config=self.input().get("config").path,
            input=self.input().get("data").path,
        )
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="json.zst"))
