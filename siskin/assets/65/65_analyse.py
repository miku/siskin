#!/usr/bin/env python3
# coding: utf-8

from __future__ import print_function

import collections
import fileinput
import json
import re
import sys

import requests

import pandas as pd
import tqdm
from six.moves import urllib


def search(query, base_url="http://10.1.1.10:8080/solr/biblio/select"):
    """
    Search SOLR, return a minimal response.
    """
    params = {
        'q': query,
        'wt': 'json',
        'rows': 0,
    }
    link = "%s?%s" % (base_url, urllib.parse.urlencode(params))
    r = requests.get(link)
    if r.status_code != 200:
        raise RuntimeError("%s on %s" % (r.status_code, link))
    return r.json()


# Map ISSN to name.
names = {}

# Count various things.
counters = collections.defaultdict(collections.Counter)

for line in tqdm.tqdm(fileinput.input(), total=941528):
    doc = json.loads(line)
    if doc["issn"] is None:
        continue

    for issn in doc.get("issn", []):
        issn = issn.upper()
        if len(issn) == 8:
            issn = issn[:4] + "-" + issn[4:]

        match = re.search(r'([0-9]{4,4}-[0-9X]{4,4})', issn)
        if not match:
            raise ValueError('failed to parse ISSN: %s', issn)
        issn = match.group(1)

        assert len(issn) == 9

        counters["c"][issn] += 1

        if isinstance(doc["name"], list):
            names[issn] = doc["name"][0]
        else:
            names[issn] = doc["name"]

        # Search ISSN in AI (w/o 65)
        if not issn in counters["ai"]:
            query = 'issn:"%s" AND NOT source_id:65' % issn
            resp = search(query)
            counters["ai"][issn] = resp["response"]["numFound"]

        # Search ISSN in AI (w/o 65) and DE-15-FID.
        if not issn in counters["fid"]:
            query = 'issn:"%s" AND NOT source_id:65 AND institution:DE-15-FID' % issn
            resp = search(query)
            counters["fid"][issn] = resp["response"]["numFound"]

data = [(names[k], k, v, counters["ai"][k], counters["fid"][k], '%0.2f%%' % (100 * float(counters["fid"][k]) / max(0.01, counters["ai"][k])))
        for k, v in counters["c"].most_common()]

df = pd.DataFrame(data, columns=["name", "issn", "count", "ai", "ai-fid", "pct"])
df.to_excel("65.xlsx")
