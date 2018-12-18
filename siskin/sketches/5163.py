#!/usr/bin/env python
# coding: utf-8

"""
Refs 5163. Download link at #note-65. Wget is your friends. 250M zipped, 860000 records.

Problems:

* Already in SOLR format. Attachments (78/2.) needs IS (for now).
* Only 5% have DOI for deduplication.

## How many ISSN?

About 638 unique ISSN.

## How many DOI?

XXX.

"""

import collections
import gzip
import re

import six
import tqdm

import ujson as json

pattern = re.compile(r'[^0-9/a-z]10[.][0-9a-zA-Z]*/[0-9a-zA-Z]{4,}')

if __name__ == '__main__':

    counter = collections.Counter()
    dois = collections.Counter()

    with gzip.open("dswarm-68-20181008150514_all.ldj.gz", "rb") as handle:
        for line in handle:
            matches = pattern.findall(line)
            doc = json.loads(line)

            for match in matches:
                match = match.strip().strip('"')
                dois[match] += 1

            output = {
                "finc.format": doc.get("format", "ElectronicArticle"),
                "finc.id": doc["id"],
                "finc.mega_collection": doc.get("mega_collection", []),
                "finc.record_id": doc["record_id"],
                "finc.source_id": doc["source_id"],
                "languages": doc.get("language", "eng"),
                "rft.atitle": doc.get("title"),
                "rft.issn": doc.get("issn", []),
                "rft.pub": doc.get("publisher"),
                "rft.place": doc.get("place"),
                "rft.date": '%s-01-01' % doc.get("publishDateSort", "1970"),
                "subjects": doc.get("topic"),
                "version": "0.9",
                "rft.authors": [{"rft.aucorp": v} for v in doc.get("author_corporate2", [])],
            }

            # Frequency of ISSN.
            for issn in output["rft.issn"]:
                counter[issn] += 1

            # print(json.dumps(output))

    print(len(counter))
    print(len(dois))

    for doi, count in dois.most_common(n=10):
        print(count, doi)
