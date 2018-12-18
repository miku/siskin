#!/usr/bin/env python
# coding: utf-8

"""
Refs 5163. Download link at #note-65. Wget is your friends. 250M zipped, 860000 records.

Usage:

    $ python 5163.py > results.json
    $ jq -rc .dois[] results.json | awk '{ print "http://doi.org/"$0 }' | clinker -w 200 > ping.json
    $ jq .status ping.json | sort | uniq -c | sort -nr
    3900 200
    132  404
    11   403

Problems:

* Already in SOLR format. Attachments (78/2.) needs IS (for now).
* Only about 5% have DOI for deduplication?

## How many ISSN?

About 638 unique ISSN.

## How many DOI?

XXX.

"""

import collections
import gzip
import re

import requests
import six
import tqdm

import ujson as json

pattern = re.compile(r'10[.][0-9a-zA-Z]*/[0-9a-zA-Z]{4,}')

if __name__ == '__main__':

    # counter = collections.Counter()
    dois = collections.Counter()

    with gzip.open("dswarm-68-20181008150514_all.ldj.gz", "rb") as handle:
        for line in tqdm.tqdm(handle):
            matches = pattern.findall(line)
            for match in matches:
                candidate = match.strip().strip('"')
                dois[candidate] += 1

            # doc = json.loads(line)
            # output = {
            #     "finc.format": doc.get("format", "ElectronicArticle"),
            #     "finc.id": doc["id"],
            #     "finc.mega_collection": doc.get("mega_collection", []),
            #     "finc.record_id": doc["record_id"],
            #     "finc.source_id": doc["source_id"],
            #     "languages": doc.get("language", "eng"),
            #     "rft.atitle": doc.get("title"),
            #     "rft.issn": doc.get("issn", []),
            #     "rft.pub": doc.get("publisher"),
            #     "rft.place": doc.get("place"),
            #     "rft.date": '%s-01-01' % doc.get("publishDateSort", "1970"),
            #     "subjects": doc.get("topic"),
            #     "version": "0.9",
            #     "rft.authors": [{"rft.aucorp": v} for v in doc.get("author_corporate2", [])],
            # }

            # Frequency of ISSN.
            # for issn in output["rft.issn"]:
            #     counter[issn] += 1

            # print(json.dumps(output))

    # print(len(counter))
    # print(len(dois))

    print(json.dumps({
        "dois": [doi for doi, _ in dois.most_common()],
        "doi_count": len(dois),
    }))
