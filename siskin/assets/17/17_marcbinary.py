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

Source: Nationallizenzen
SID: 17
Ticket: #15788, #15863, ##15865

"""

import re
import sys
from io import BytesIO, StringIO

import marcx
import pymarc
from siskin.utils import marc_clean_record, xmlstream

adlr_ddc = ["175", "303.38", "342.0853", "384", "778.5", "791"]

adlr_ddc_start = [
    "002", "070", "302.2", "303.375", "303.376", "303.4833", "303.4834", "323.445", "324.73", "343.099", "384.3", "384.54", "384.55", "384.8", "659", "741.5",
    "770", "781.54", "791.4", "794.8"
]

inputfilename = "17_input.xml"
outputfilename = "17_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

outputfile = open(outputfilename, "wb")

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
    record.add("001", data="17-" + f001)

    # Ansigelung und Kollektion
    try:
        f082a = record["082"]["a"]
    except:
        f082a = ""

    for ddc in adlr_ddc_start:
        if f082a.startswith(ddc):
            f082a_startswith_adlr_ddc = True
            break
    else:
        f082a_startswith_adlr_ddc = False

    f912a = record.get_fields("912")

    if len(f912a) == 2:
        f912a_1 = f912a[0].get_subfields("a")[0].lower()
        f912a_2 = f912a[1].get_subfields("a")[0].lower()
    else:
        f912a_1 = f912a[0].get_subfields("a")[0].lower()
        f912a_2 = ""

    if f082a in adlr_ddc or f082a_startswith_adlr_ddc:
        collections = ["a", f001, "b", "17", "c", f912a_1, "c", f912a_2, "c", "sid-17-col-adlr"]
    else:
        collections = ["a", f001, "b", "17", "c", f912a_1, "c", f912a_2]

    record.add("980", subfields=collections)

    marc_clean_record(record)
    outputfile.write(record.as_marc())

outputfile.close()
