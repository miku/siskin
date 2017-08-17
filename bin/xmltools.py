#!/usr/bin/env python3
# coding: utf-8
# pylint: disable=C0103

# Copyright 2017 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
#                   Robert Schenk, <robert.schenk@uni-leipzig.de>
#                   Martin Czygan, <martin.czygan@uni-leipzig.de>
#
# This file is part of some open source application.
#
# Some open source application is free software: you can redistribute
# it and/or modify it under the terms of the GNU General Public
# License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.
#
# Some open source application is distributed in the hope that it will
# be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Foobar. If not, see <http://www.gnu.org/licenses/>.
#
# @license GPL-3.0+ <http://spdx.org/licenses/GPL-3.0+>



# Contains a collection of utilities for parsing and analysing xml files
# Current functions: list_fields, count_records
# To do: sort tags, count_doublet, print_json


import collections
import argparse
import xml.sax
import json
import os
import re
import subprocess


parser = argparse.ArgumentParser()
parser.add_argument("-v",
                    action="version",
                    help="show version",
                    version="0.0.1")
parser.add_argument("-f",
                    dest="inputfile",
                    help="file to parse",
                    metavar="filename")
parser.add_argument("-t",
                    dest="task",
                    help="task to do (list_fields, count_records, print_json)",
                    metavar="task")
args = parser.parse_args()

if args.task == "list_fields":

    class RecordHandler(xml.sax.ContentHandler):

        def __init__(self):
            self.fieldlist = {}
            self.current = (None, None)

        def startElement(self, name, attr):
            self.current = (name, attr)

        def characters(self, value):
            name = self.current[0]
            value = value.strip()
            if value:
                if name not in self.fieldlist:
                    self.fieldlist[name] = value

    parser = xml.sax.make_parser()
    handler = RecordHandler()
    parser.setContentHandler(handler)
    parser.parse(open(args.inputfile, "r"))

    fieldlist = handler.fieldlist
    for key, value in fieldlist.items():
        print(key + " = " + value)

elif args.task == "print_json":

    class RecordHandler(xml.sax.ContentHandler):

        def __init__(self):
            self.data = collections.defaultdict(list)
            self.current = (None, None)

        def startElement(self, name, attr):
            self.current = (name, attr)

        def endElement(self, name):
            print(json.dumps(self.data))
            self.data.clear()

        def characters(self, value):
            name = self.current[0]
            value = value.strip()
            if value:
                self.data[name].append(value)

    parser = xml.sax.make_parser()
    handler = RecordHandler()
    parser.setContentHandler(handler)
    parser.parse(open(args.inputfile, "r"))

elif args.task == "count_records":

    file = open(args.inputfile, "r", encoding="utf-8")
    last_line = ""
    for line in file:
        forelast_line = last_line
        last_line = line

    regexp = re.match(".*</(.*)>", forelast_line)
    if regexp:
        forelast_tag = regexp.group(1)
    else:
        forelast_tag = forelast_line

    command = 'grep -c "</%s>" %s' % (forelast_tag, args.inputfile)
    os.system(command)