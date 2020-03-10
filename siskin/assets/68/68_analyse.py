#!/usr/bin/env python3
# coding: utf-8

from __future__ import print_function

import collections
import io
import json
import re
import sys

import requests

import pandas as pd
import tqdm
from six.moves import urllib

if len(sys.argv) != 3:
    raise ("Es muss eine Inputdatei und eine URL angegeben werden!")

inputfilename, base_url = sys.argv[1:]

inputfile = io.open(inputfilename, "r")


def search(query, base_url):
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

for line in inputfile:

    doc = json.loads(line)

    if not (doc.get("title") and doc.get("issn")):
        continue

    for issn in doc.get("issn", []):
        if not issn:
            continue
        issn = issn.upper()
        if len(issn) == 8:
            issn = issn[:4] + "-" + issn[4:]

        match = re.search(r'([0-9]{4,4}-[0-9X]{4,4})', issn)
        if not match:
            raise ValueError('failed to parse ISSN: %s', issn)
        issn = match.group(1)

        assert len(issn) == 9

        counters["c"][issn] += 1

        if isinstance(doc["title"], list):
            names[issn] = doc["title"][0]
        else:
            names[issn] = doc["title"]

        # Search ISSN in AI (w/o 68)
        if not issn in counters["ai"]:
            query = 'issn:"%s" AND NOT source_id:68' % issn
            resp = search(query, base_url)
            counters["ai"][issn] = resp["response"]["numFound"]

        # Search ISSN in AI (w/o 68) and DE-15-FID.
        if not issn in counters["fid"]:
            query = 'issn:"%s" AND NOT source_id:68 AND institution:DE-15-FID' % issn
            resp = search(query, base_url)
            counters["fid"][issn] = resp["response"]["numFound"]

data = [(names[issn], issn, freq, counters["ai"][issn], counters["fid"][issn],
         '%0.2f%%' % (100 * float(counters["fid"][issn]) / max(0.01, counters["ai"][issn]))) for issn, freq in counters["c"].most_common()]

df = pd.DataFrame(data, columns=["title", "issn", "count", "ai", "ai-fid", "pct"])
df.to_excel("68.xlsx")
