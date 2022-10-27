#!/usr/bin/env python
# coding: utf-8
"""
Refs 5163. Download link at #note-65. Wget is your friends. 250M zipped, 860000 records.

Rough steps:

    # Find resolvable DOI.
    $ python 5163.py > results.json
    $ jq -rc .dois[] results.json | awk '{ print "http://doi.org/"$0 }' | clinker -w 200 > ping.json
    $ jq .status ping.json | sort | uniq -c | sort -nr
    3900 200
    132  404
    11   403

    # How many would be FID-MEDIEN-DE-15?
    $ python 5163.py > 68.is
    $ span-tag -unfreeze $(taskoutput AMSLFilterConfigFreeze) 68.is > 68.tagged.is
    $ jq -rc '.["x.labels"][]?' 68.tagged.is | wc -l
    463843

    # How many URL?
    $ jq -rc .url 68.is | grep -c -F '[]'
    827810

Problems:

* Already in SOLR format. Attachments (78/2.) needs IS (for now).
* Only about 5% have DOI for deduplication?
* No URL?

## How many ISSN?

About 638 unique ISSN.

## How many DOI?

About 3900.

"""

import collections
import gzip
import json
import re

import requests
import six
import tqdm

pattern = re.compile(r"10[.][0-9a-zA-Z]*/[0-9a-zA-Z]{4,}")

if __name__ == "__main__":

    counter = collections.Counter()
    resolvable_dois = set()

    # Not all DOI matches are actual DOIs
    with open("ping.json") as handle:
        for line in handle:
            doc = json.loads(line)
            if not doc["status"] == 200:
                continue
            resolvable_dois = doc["link"].replace("http://doi.org/", "")

    with gzip.open("dswarm-68-20181008150514_all.ldj.gz", "rb") as handle:
        for line in tqdm.tqdm(handle):
            candidates = set([match.strip().strip('"') for match in pattern.findall(line)])
            resolvable = [doi for doi in candidates if doi in resolvable_dois]
            doc = json.loads(line)

            authors = [{"rft.aucorp": v} for v in doc.get("author_corporate2", [])]
            authors = authors + [{"rft.au": v} for v in doc.get("author", [])]
            authors = authors + [{"rft.au": v} for v in doc.get("author2", [])]

            output = {
                "finc.format": doc.get("format", ["ElectronicArticle"])[0],
                "finc.id": doc["id"],
                "finc.mega_collection": doc.get("mega_collection", []),
                "finc.record_id": doc["record_id"],
                "finc.source_id": doc["source_id"],
                "languages": doc.get("language", ["eng"]),
                "rft.atitle": doc.get("title", ""),
                "rft.issn": doc.get("issn", []),
                "rft.pub": doc.get("publisher", []),
                "rft.place": doc.get("place", []),
                "rft.date": "%s-01-01" % doc.get("publishDateSort", "1970"),
                "x.subjects": doc.get("topic", []),
                "version": "0.9",
                "rft.authors": authors,
            }
            if resolvable:
                output["doi"] = resolvable[0]

            output["url"] = doc.get("url", [])
            if not output["url"] and output.get("doi"):
                output["url"] = "https://doi.org/{}".format(output["doi"])

            # Frequency of ISSN.
            # for issn in output["rft.issn"]:
            #     counter[issn] += 1

            print(json.dumps(output))

    # print(len(counter))
    # print(len(dois))
    # print(json.dumps({
    #     "dois": [doi for doi, _ in dois.most_common()],
    #     "doi_count": len(dois),
    # }))
