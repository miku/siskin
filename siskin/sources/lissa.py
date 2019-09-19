# coding: utf-8
# pylint: disable=C0301,E1101

# Copyright 2019 by Leipzig University Library, http://ub.uni-leipzig.de
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
LISSA, https://lissarchive.org/

> A free, open scholarly platform for library and information science,
https://osf.io/preprints/lissa/discover

Source: 179
Ticket: 15839
"""

import datetime
import itertools
import json

import langdetect
from iso639 import languages

import luigi
from gluish.format import Gzip
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask


class LissaTask(DefaultTask):
    """
    Base task.
    """
    TAG = '179'

    def closest(self):
        return weekly(date=self.date)


class LissaFetch(LissaTask):
    """
    Fetch dataset via single curl request with large size to ES.

    Fixed endpoint at: https://share.osf.io/api/v2/search/creativeworks/

    In 08/2019, there are only about 230 items, so a single request window of
    1000 suffices. Adjust, if dataset grows.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        es = "https://share.osf.io/api/v2/search/creativeworks"
        query = "sources:%22LIS%20Scholarship%20Archive%22"
        output = shellout("""curl -s --fail "{es}/_search?from=0&size=1000&q={query}" > {output}""", query=query, es=es)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="json"))


class LissaIntermediateSchema(LissaTask):
    """
    Convert custom JSON to intermediate schema.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return LissaFetch(date=self.date)

    def run(self):
        with self.input().open() as handle:
            resp = json.load(handle)

        converted = []

        for hit in resp["hits"]["hits"]:
            source = hit["_source"]
            doc = {
                "finc.id": "ai-179-{}".format(source["id"]),
                "finc.source_id": "179",
                "finc.format": "ElectronicArticle",
                "finc.record_id": source["id"],
                "finc.mega_collection": ["LISSA", "sid-179-col-lissa"],
                "ris.type": "EJOUR",
                "rft.atitle": source["title"],
                "rft.genre": "article",
                "rft.pub": source.get("publishers", []),
                "authors": [{
                    "rft.au": name
                } for name in source["contributors"]],
                "url": [link for link in source["identifiers"] if link.startswith("http")],
                "abstract": source.get("description", ""),
            }

            dois = [v.replace("http://dx.doi.org/", "") for v in source["identifiers"] if "doi.org" in v]
            if len(dois) == 0:
                self.logger.warn("document without DOI")
            elif len(dois) == 1:
                doc.update({"doi": dois[0]})
            else:
                # In 08/2019, various DOI seem to work.
                self.logger.warn("document with multiple dois: %s", dois)
                doc.update({"doi": dois[0]})

            if doc.get("language"):
                doc.update({"language": doc.get("language")})
            else:
                if len(doc["abstract"]) > 20:
                    result = langdetect.detect(doc["abstract"])
                    doc["languages"] = [languages.get(alpha2=result).bibliographic]
                    self.logger.debug("detected %s in abstract (%s)", doc["languages"], doc["abstract"][:40])

            # Gather subjects.
            subjects = source.get("subjects", []) + source.get("subject_synonyms", []) + source.get("tags", [])
            unique_subjects = set(itertools.chain(*[v.split("|") for v in subjects]))
            doc.update({"x.subjects": list(unique_subjects)})

            # Try date_published, then date_created, then fail.
            for key in ("date_published", "date_created"):
                if key not in source or not source[key]:
                    continue
                doc.update({
                    "x.date": source[key][:19] + "Z",
                    "rft.date": source[key][:10],
                })
                break
            else:
                raise ValueError("did not find any date field in document", hit)

            converted.append(doc)

        with self.output().open('w') as output:
            for doc in converted:
                serialized = json.dumps(doc) + "\n"
                output.write(serialized.encode("utf-8"))

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="ldj.gz"), format=Gzip)
