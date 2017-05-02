#!/usr/bin/env python
# coding: utf-8
# pylint: disable=C0103,E1123

"""

Given a list of titles or ISSN, find the database name. Use an elasticsearch
index loaded with WISO intermediate schema data.

    $ esbulk -index wiso -id finc.record_id -z $(taskoutput GeniosCombinedIntermediateSchema)

Example:

    $ xlsx2tsv.py ..._Fachzeitschriften_Psychologie_2017.xlsx | cut -f1 | grep -i jour | \
    python contrib/gbi_guess_package.py --jtitle -
    ["JBS"]
    ["JBS"]
    ["JBS"]
    ["JBS"]

"""

from __future__ import print_function

import argparse
import fileinput
import json

import elasticsearch

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--issn', action='store_true', help='find database names for issn list')
    parser.add_argument('--jtitle', action='store_true',
                        help='find database names for journal title list')
    parser.add_argument('files', metavar='FILE', nargs='*',
                        help='files to read, if empty, stdin is used')

    args = parser.parse_args()

    es = elasticsearch.Elasticsearch()

    if args.issn:
        for line in fileinput.input(files=args.files if len(args.files) > 0 else ('-', )):
            result = es.search(index="wiso", q="""rft.issn:"%s" """ % line.strip())
            hits = result.get('hits')
            for hit in hits.get('hits'):
                print(json.dumps(hit["_source"]["x.packages"]))

    if args.jtitle:
        for line in fileinput.input(files=args.files if len(args.files) > 0 else ('-', )):
            result = es.search(index="wiso", q="""  rft.jtitle:"%s" """ % line.strip())
            hits = result.get('hits')
            for hit in hits.get('hits'):
                print(json.dumps(hit["_source"]["x.packages"]))
