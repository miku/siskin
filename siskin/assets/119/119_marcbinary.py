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

Source: Universitätsbibliothek Frankfurt am Main (VK Film)
SID: 119
Ticket: #8571, #14654, #15877

"""

import io
import re
import sys

import marcx
import pymarc
from siskin.utils import marc_clean_record

inputfilename = "119_input.mrc"
outputfilename = "119_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = open(inputfilename, "rb")
outputfile = open(outputfilename, "wb")

reader = pymarc.MARCReader(inputfile)

for record in reader:

    record = marcx.Record.from_record(record)
    record.force_utf8 = True
    record.strict = False

    # Identifikator
    try:
        f001 = record["001"].data
    except:
        print("geht nicht")
        f001 = "x"
    record.remove_fields("001")
    record.add("001", data="finc-119-" + f001)

    # URL
    try:
        type = record["024"]["2"]
        url = record["024"]["a"]
    except:
        type = ""
        url = ""
    if url and type == "doi" or type == "urn":
        if url.startswith("10."):
            url = "doi.org/" + url
        record.remove_fields("856")
        record.add("856", q="text/html", _3="Link zur Ressource", u=url)

    # Kollektion
    record.remove_fields("912")
    record.add("912", a="vkfilm")

    # Ansigelung
    collections = ["a", f001, "b", "119", "c", "sid-119-col-ubfrankfurt"]
    record.add("980", subfields=collections)

    marc_clean_record(record)
    outputfile.write(record.as_marc())

outputfile.close()
