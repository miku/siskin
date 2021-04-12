#!/usr/bin/env python
# coding: utf-8
#
# Copyright 2019 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
#                   Martin Czygan, <martin.czygan@uni-leipzig.de>
#                   Robert Schenk, <robert.schenk@uni-leipzig.de>
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
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
#
# @license GPL-3.0+ <http://spdx.org/licenses/GPL-3.0+>
"""

Source: Online Contents (OLC)
SID: 68
Ticket: #5163, #6743, #9354, #10294, #16196

"""

import json
import re
import sys
from io import BytesIO, StringIO

from siskin.utils import marc_clean_record, xmlstream

import marcx
import pymarc

inputfilename = "68_input.xml"
outputfilename = "68_output.mrc"
issnlistfilename = "title_list"

if len(sys.argv) == 4:
    inputfilename, outputfilename, issnlistfilename = sys.argv[1:]

outputfile = open(outputfilename, "wb")
issnlistfile = open(issnlistfilename, "r")
issn_list = [line.strip() for line in issnlistfile]

ids = set()

for x in issn_list:
    x = json.loads(x)
    try:
        issns = x["issn"]
    except:
        continue
    try:
        #title = x["title_short"]
        title = x["title"]
    except:
        continue

    title = title.replace(" ", "")
    title = title.replace(",", "")
    title = title.replace(";", "")
    title = title.replace(".", "")
    title = title.replace("?", "")
    title = title.replace("!", "")
    title = title.replace(":", "")
    title = title.replace("/", "")
    title = title.replace(")", "")
    title = title.replace("(", "")
    title = title.replace("-", "")
    title = title.lower()
    #title = title.split(":")[0]

    for issn in issns:
        issn = issn.replace("-", "")
        id = issn + title
        ids.add(id)

for oldrecord in xmlstream(inputfilename, "record"):

    oldrecord = BytesIO(oldrecord)
    oldrecord = pymarc.marcxml.parse_xml_to_array(oldrecord)
    oldrecord = oldrecord[0]

    record = marcx.Record.from_record(oldrecord)
    record.force_utf8 = True
    record.strict = False

    # Identifikator
    f001 = record["001"].data
    record.remove_fields("001")
    record.add("001", data="68-" + f001)

    # Zugangstyp
    record.add("007", data="cr")

    # ISSN-Check
    try:
        issns = record.get_fields("773")
    except:
        issns = []

    try:
        title = record["245"]["a"]
    except:
        continue

    try:
        subtitle = record["245"]["b"]
    except:
        subtitle = ""

    if not subtitle:
        subtitle = ""
    title = title + subtitle

    title = title.replace(" ", "")
    title = title.replace(",", "")
    title = title.replace(";", "")
    title = title.replace(".", "")
    title = title.replace("?", "")
    title = title.replace("!", "")
    title = title.replace(":", "")
    title = title.replace("/", "")
    title = title.replace(")", "")
    title = title.replace("(", "")
    title = title.replace("-", "")
    title = title.lower()
    #title = title.split(":")[0]

    if issns:
        for issn in issns:
            issn = issn.get_subfields("x")
            if issn:
                issn = issn[0]
                issn = issn.replace("-", "")
                id = issn + title
                print(id)
                if id in ids:
                    continue
    else:
        continue

    # Ansigelung und Kollektion
    record.remove_fields("980")
    fields = record.get_fields("912")
    if fields:
        for field in fields:
            f912a = field.get_subfields("a")[0]
            if "SSG-OLC-MKW" in f912a:
                record.add("980", a=f001, b="68", c="sid-68-col-olcmkw")
                break
            elif "SSG-OLC-FTH" in f912a:
                record.add("980", a=f001, b="68", c="sid-68-col-olcfth")
                break
        else:
            continue

    marc_clean_record(record)
    outputfile.write(record.as_marc())

outputfile.close()
