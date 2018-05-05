# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301
# Copyright 2018 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
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
OADOI dataset.


Config
------

[oadoi]

dump = /path/to/full_dois_2018-03-29T113154.jsonl

----

Priem, Jason and Piwowar, Heather. (2018) The Unpaywall Dataset. DOI:
https://doi.org/10.6084/m9.figshare.6020078

> File description
> name: full_dois_2018-03-29T113154.jsonl
> description: This file has one row per DOI, with the best OA location for every DOI, as well as some metadata for each DOI.
> size: 16GB (83G unzipped)
> lines: 95,842,233 lines 
> lines with at least one Open Access URL: ~16 million

Sample row:

    {
        "doi": "10.4135/9781412950534.n2123",
        "year": null,
        "genre": "reference-entry",
        "is_oa": false,
        "title": "Sport Skill Training",
        "doi_url": "https://doi.org/10.4135/9781412950534.n2123",
        "updated": "2018-03-27T15:17:42.869680",
        "publisher": "SAGE Publications, Inc.",
        "z_authors": [
            {
                "given": "David",
                "family": "Reitman"
            },
            {
                "given": "Stephen D. A.",
                "family": "Hupp"
            },
            {
                "given": "Patrick M.",
                "family": "O'Callaghan"
            }
        ],
        "journal_name": "Encyclopedia of Behavior Modification and Cognitive ...",
        "oa_locations": [],
        "data_standard": 1,
        "journal_is_oa": false,
        "journal_issns": null,
        "published_date": null,
        "best_oa_location": null,
        "journal_is_in_doaj": false,
        "x_reported_noncompliant_copies": []
    }

"""

import luigi
from luigi.format import Gzip
from gluish.utils import shellout
from siskin.task import DefaultTask


class OADOITask(DefaultTask):
    """
    Base task for oadoi.
    """
    TAG = 'oadoi'


class OADOIDump(OADOITask, luigi.ExternalTask):
    """
    Provided data dump. See also release notes at:

    * https://docs.google.com/document/d/1Whfe26oyjTedeW1GGWkq3NADgDbL2R2eXqrJCWS8vcc/edit#
    """

    def output(self):
        return luigi.LocalTarget(path=self.config.get("oadoi", "dump"), format=Gzip)


class OADOIList(OADOIDump):
    """
    Provide a simple CSV (doi, is_oa).
    """

    def requires(self):
        return OADOIDump()

    def run(self):
        """
        XXX: parse error: Invalid numeric literal at line 12536737, column 0
        """
        output = shellout("unpigz -c {input} | jq -rc '[.doi, .is_oa] | @csv' | pigz -c > {output}",
                          input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="csv.gz"), format=Gzip)
