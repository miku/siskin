# coding: utf-8
# pylint: disable=C0301,E1101

# Copyright 2022 by Leipzig University Library, http://ub.uni-leipzig.de
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
Refs. #16327
"""

import datetime
import functools
import os

import luigi
import requests
from gluish.format import Zstd
from gluish.utils import shellout

from siskin.task import DefaultTask


class KalliopeTask(DefaultTask):
    """
    Base task. We only care for a recent download. Only works, if there's a
    file the download URL at all.
    """

    last_modified = luigi.Parameter(default="", description="format: 2022-08-25, overrides last-modified header check", significant=False)

    TAG = "140"
    download_url = "https://download.ubl-proxy.slub-dresden.de/kalliope"

    @functools.lru_cache
    def get_last_modified_date(self):
        if self.last_modified:
            return self.last_modified
        self.logger.debug("kalliope: last-modified check")
        resp = requests.head(self.download_url)
        last_modified_header = resp.headers.get("Last-Modified")
        if last_modified_header is None:
            raise KeyError("last-modified header not found, got HTTP {} on {}".format(resp.status_code, self.download_url))
        last_modified = datetime.datetime.strptime(last_modified_header, "%a, %d %b %Y %H:%M:%S %Z")
        return last_modified


class KalliopeDirectDownload(KalliopeTask):
    """
    Download.
    """

    url = luigi.Parameter(default=KalliopeTask.download_url, significant=False)

    def run(self):
        if "kalliope-" not in os.path.basename(self.output().path):
            raise RuntimeError("url not usable: {}".format(self.download_url))
        output = shellout(
            """ curl --output {output} --fail -sL "{url}" """,
            url=self.url,
        )
        # in AMSL we have:
        #   "collection": [
        #     "Nachlässe SLUB Dresden",
        #     "sid-140-col-nachlaesseslub"
        #   ]
        # data has:
        #   "mega_collection": ["Nachlässe, Vorlässe, Sammlungen SLUB Dresden"],
        output = shellout(
            """ tar -xOzf {input} |
                jq -rc '.mega_collection += ["sid-140-col-nachlaesseslub"]' |
                zstd -c -T0 > {output} """,
            input=output,
        )
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        try:
            last_modified = self.get_last_modified_date()
            filename = "kalliope-{}.zst".format(last_modified.strftime("%Y-%m-%d"))
            return luigi.LocalTarget(path=self.path(filename=filename), format=Zstd)
        except KeyError as exc:
            self.logger.warn("unuable URL, will trigger an exception on run")
            return luigi.LocalTarget(is_tmp=True) # just a dummy, we'll use that to trigger an exception in run
