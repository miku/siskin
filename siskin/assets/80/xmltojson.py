#!/usr/bin/env python
# coding: utf-8
# pylint: disable=C0103

"""
Convert XML to JSON, standalone script.
"""

import json
import sys

import xmltodict

if __name__ == '__main__':
    if len(sys.argv) == 3:
        with open(sys.argv[2], 'w') as output:
            with open(sys.argv[1], 'r') as handle:
                doc = xmltodict.parse(handle.read())
                json.dump(doc, output)
    else:
        print("%s %s %s" % (sys.argv[0], 'input.xml', 'output.json'))
