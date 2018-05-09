#!/usr/bin/env python
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
"""

import argparse
import json
import logging

import requests
from six.moves.urllib.parse import urlencode

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %H:%M:%S')


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

if __name__ == '__main__':
    parser = argparse.ArgumentParser("12756")
    parser.add_argument("--server", "-r", type=str, default="http://localhost:8983/solr/biblio")
    parser.add_argument("--field", "-f", type=str, default="format")
    parser.add_argument("--source-id", "-s", type=str, default="49")
    args = parser.parse_args()

    solr = Solr(server=args.server)

    query = "source_id:%s" % (args.source_id)
    resp = solr.facets(field=args.field, q=query)
    for name, count in facet_response_values(resp):
        print("%s\t%s" % (name, count))

