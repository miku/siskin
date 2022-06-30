#!/usr/bin/env python3
# coding: utf-8

import collections
import json
import re

import pandas as pd

filename = "65.ldj"

names = {}
c = collections.Counter()

with open(filename) as file:
    for line in file:
        data = json.loads(line)
        if data["issn"] is None:
            continue

        for issn in data.get("issn", []):
            issn = issn.upper()
            if len(issn) == 8:
                issn = issn[:4] + "-" + issn[4:]

            match = re.search(r"([0-9]{4,4}-[0-9X]{4,4})", issn)
            if not match:
                raise ValueError("failed to parse ISSN: %s", issn)
            issn = match.group(1)

            assert len(issn) == 9

            c[issn] += 1
            if isinstance(data["name"], list):
                names[issn] = data["name"][0]
            else:
                names[issn] = data["name"]

    df = pd.DataFrame(
        [(names[k], k, v) for k, v in c.most_common()],
        columns=["name", "issn", "count"],
    )
    df.to_excel("65.xlsx")
