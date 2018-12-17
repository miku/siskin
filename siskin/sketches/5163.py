#!/usr/bin/env python
# coding: utf-8

"""
Refs 5163. Download link at #note-65. Wget is your friends. 250M zipped, 860000 records.

Problems:

* Already in SOLR format. Attachments (78/2.) needs IS (for now).
* Only 5% have DOI for deduplication.

"""

import gzip
import ujson as json
import tqdm

if __name__ == '__main__':
    with gzip.open("dswarm-68-20181008150514_all.ldj.gz", "rb") as handle:
        for line in tqdm.tqdm(handle):
            doc = json.loads(line)
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
            print(json.dumps(output))

