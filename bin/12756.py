#!/usr/bin/env python3
# coding: utf-8

"""
Checks, refs #12756.

Example:

* format: free, online on open access source
* year: plausible
* crossref: newlines in crossref
* finc_class_facet: examples
* link check samples
* difference increase

Example:

    $ bin/12756.py -s 55 -f mega_collection
    ...

    $ bin/12756.py -s 89 -f author_facet | csvlook -t -H
    ...

    $ bin/12756.py --server http://localhost:8983/solr/biblio --all
    05/09/2018 17:14:49 [28] [publishDateSort]: 2019 (98)
    05/09/2018 17:14:50 [30] [publishDateSort]: 1004 (1)
    05/09/2018 17:14:52 [48] [author_facet]: Copyright 2004 Thomson Media Inc. All Rights Reserved. http: (14888)
    05/09/2018 17:14:52 [48] [author_facet]: Copyright 2003 Thomson Media Inc. All Rights Reserved. http: (14259)
    05/09/2018 17:14:52 [48] [author_facet]: Copyright c 2001 Thomson Financial. All Rights Reserved. http: (13903)
    05/09/2018 17:15:09 [53] [author_facet]: The Polish Institute of International Affairs, Editorial Board (104)
    05/09/2018 17:15:13 [87] [author_facet]: Chan, Lik Sam; Annenberg School for Communication and Journalism, USC (5)
    05/09/2018 17:15:13 [87] [author_facet]: Hannah, Mark; University of Southern California, Annenberg School (4)
    05/09/2018 17:15:13 [87] [author_facet]: Mbunyuza-Memani, Lindani; Southern Illinois University, Carbondale (4)
    05/09/2018 17:15:13 [87] [author_facet]: Stephens, Niall P.; Department of Communication Arts Framingham State University Framingham Massachusetts, USA (4)
    05/09/2018 17:15:13 [87] [author_facet]: Anima-Korang, Angela; Southern Illinois University Carbondale (3)
    05/09/2018 17:15:13 [87] [author_facet]: Faulhaber, Gerald R.; Wharton School, University of Pennsylvania (3)
    05/09/2018 17:15:13 [87] [author_facet]: Pamment, James; University of Texas at Austin & Karlstad University, Sweden (3)
    05/09/2018 17:15:13 [87] [author_facet]: Aufderheide, Patricia; Center for Social Media, American University (2)
    05/09/2018 17:15:13 [87] [author_facet]: Aufderheide, Patricia; School of Communication, American University (2)
    05/09/2018 17:15:13 [87] [author_facet]: Bai, Yang; College of Communications Pennsylvania State University (2)
    05/09/2018 17:15:13 [87] [author_facet]: Brantner, Cornelia; Department of Communication, University of Vienna (2)
    05/09/2018 17:15:16 [105] [publishDateSort]: 1 (1)

"""

import argparse
import datetime
import json
import logging
import sys

import requests
from six.moves.urllib.parse import urlencode

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %H:%M:%S"
)


def facet_response_values(resp):
    """
    Turn a SOLR facet response into a simple sequence of values and counts.
    """
    ff = resp["facet_counts"]["facet_fields"]
    for _, v in ff.items():
        for i in range(0, len(v), 2):
            yield v[i], v[i + 1]


class Solr(object):
    """
    Wrap a solr server and expose a couple of helper methods.
    """

    def __init__(self, server="http://localhost:8983/solr/biblio"):
        self.server = server.rstrip("/")

    def facets(self, q="source_id:49", field="format"):
        """
        Return facet values and count.
        """
        params = {
            "wt": "json",
            "facet": "on",
            "facet.field": field,
            "facet.mincount": 1,
            "facet.limit": -1,
            "q": q,
            "rows": 0,
        }
        link = "%s/select?%s" % (self.server, urlencode(params))
        logging.debug(link)

        r = requests.get(link)
        if not r.status_code == 200:
            raise RuntimeError("request failed with %s: %s", r.status_code, link)
        resp = json.loads(r.text)
        return resp


def test_year_is_plausible(year):
    try:
        v = int(year)
        if not 1500 < v < datetime.date.today().year + 1:
            logging.warn("implaubible year: %s", year)
    except ValueError:
        logging.warn("implausible year: %s", year)


def test_field(resp, field, test_func):
    vcs = [(v, c) for v, c in facet_response_values(resp)]
    for v, c in vcs:
        if not test_func(v):
            logging.warn("[%s] [%s]: %s (%s)", source_id, field, v, c)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("12756")
    parser.add_argument(
        "--server", "-r", type=str, default="http://localhost:8983/solr/biblio"
    )
    parser.add_argument("--field", "-f", type=str, default="format")
    parser.add_argument("--source-id", "-s", type=str, default="49")
    parser.add_argument(
        "--all", action="store_true", help="Run all queries from #12756#note-2"
    )

    args = parser.parse_args()

    solr = Solr(server=args.server)

    if args.all:
        source_ids = (28, 30, 34, 48, 49, 50, 53, 55, 60, 85, 87, 89, 101, 105)
        fields = (
            "format",
            "format_de15",
            "facet_avail",
            "access_facet",
            "author_facet",
            "publishDateSort",
            "language",
            "mega_collection",
            "finc_class_facet",
        )

        for source_id in source_ids:
            query = "source_id:%s" % (source_id)
            for field in fields:
                resp = solr.facets(field=field, q=query)

                # Field specific tests.
                if field == "publishDateSort":
                    test_field(resp, field, test_year_is_plausible)

                if field == "author_facet":
                    test_field(resp, field, lambda v: len(v) < 60)

                if field == "mega_collection":
                    test_field(resp, field, lambda v: "??" not in v)

                # Just output everything.
                for value, count in facet_response_values(resp):
                    pass  # print("%s\t%s" % (value, count))

        sys.exit(0)

    query = "source_id:%s" % (args.source_id)
    resp = solr.facets(field=args.field, q=query)
    for name, count in facet_response_values(resp):
        print("%s\t%s" % (name, count))
