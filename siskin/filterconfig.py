# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301,C0103,W0614,W0401,E0202

# Copyright 2023 by Leipzig University Library, http://ub.uni-leipzig.de
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
Helper for label attachment configurations.

AMSL service can build a list of attachments:

    {
      "DokumentLabel": "KBART_DE-Gla1",
      "DokumentURI": "http://amsl.technology/discovery/metadata-usage/Dokument/KBART_DEGla1",
      "evaluateHoldingsFileForLibrary": "yes",
      "ISIL": "DE-D275",
      "linkToHoldingsFile": "https://live.amsl.technology/OntoWiki/files/get?setResource=http://amsl.technology/discovery/metadata-usage/Dokument/KBART_DEGla1",
      "megaCollection": "Asociacion Colombiana de Cirugia (CrossRef)",
      "productISIL": "",
      "shardLabel": "UBL-ai",
      "sourceID": "49",
      "technicalCollectionID": ""
    }

"""

import collections
import json
import logging
import sys
from typing import List, Optional

from pydantic import BaseModel, Field

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)

logger = logging.getLogger("siskin")
logger.setLevel(logging.DEBUG)
logger.addHandler(ch)


class Service(BaseModel):
    label: Optional[str] = Field(alias="DokumentLabel")
    uri: Optional[str] = Field(alias="DokumentURI")
    evaluate_holdings_file_for_library: str = Field(
        alias="evaluateHoldingsFileForLibrary"
    )
    isil: str = Field(alias="ISIL")
    link_to_holdings_file: Optional[str] = Field(alias="linkToHoldingsFile")
    mega_collection: str = Field(alias="megaCollection")
    product_isil: str = Field(alias="productISIL")
    share_label: str = Field(alias="shardLabel")
    source_id: str = Field(alias="sourceID")
    technical_collection_id: str = Field(alias="technicalCollectionID")


class ServiceResponse(BaseModel):
    """
    AMSL Service response, about 250MB currently.
    """

    docs: List[Service]

    def filter_by(self, **kwargs):
        for doc in self.docs:
            for k, v in kwargs.items():
                if not getattr(doc, k) == v:
                    continue
                yield doc

    def describe(self):
        counter = collections.defaultdict(lambda: collections.Counter())
        for doc in self.docs:
            counter[doc.isil][doc.source_id] += 1

        return counter

    def dump_isil_sources(self):
        for k, vs in self.describe().items():
            for sid, count in vs.items():
                print(f"{k:20s}{sid:10s}{count:10d}")

    def generate_filter_config():
        """
        Returns a dictionary in filterconfig format, top-level keys are ISIL.
        """
        pass


# TODO: generate filter config; group collections, except from [49]

if __name__ == "__main__":
    payload = json.load(sys.stdin)
    logger.info("parsing payload")
    docs = (Service(**doc) for doc in payload)
    logger.info("parsing response")
    resp = ServiceResponse(docs=docs)
    logger.info("done")
    resp.dump_isil_sources()
