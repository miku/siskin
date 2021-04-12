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

# Source: K10plus Verbundkatalog (für spezielle Profilierung)
# SID: 183
# Ticket: #15694, #15752, #15833

"""

import os
import sys
from io import BytesIO, StringIO

from siskin.mappings import formats
from siskin.utils import marc_clean_record, xmlstream

import marcx
import pymarc

input_directory = "input"
outputfilename = "183_output.mrc"

if len(sys.argv) == 3:
    input_directory, outputfilename = sys.argv[1:]

outputfile = open(outputfilename, "wb")

for root, _, files in os.walk(input_directory):

    for filename in files:
        if not filename.endswith(".xml"):
            continue
        inputfilepath = os.path.join(root, filename)

        for oldrecord in xmlstream(inputfilepath, "record"):

            oldrecord = BytesIO(oldrecord)
            oldrecord = pymarc.marcxml.parse_xml_to_array(oldrecord)
            oldrecord = oldrecord[0]

            marcrecord = marcx.Record.from_record(oldrecord)
            marcrecord.force_utf8 = True
            marcrecord.strict = False

            try:
                marcrecord["245"]["a"]
            except:
                continue

            # Identifikator
            f001 = marcrecord["001"].data
            marcrecord.remove_fields("001")
            marcrecord.add("001", data="183-" + f001)

            # Ansigelung und Kollektion
            collections = ["a", f001, "b", "183", "c", "sid-183-col-kxpbbi"]
            marcrecord.add("980", subfields=collections)

            marc_clean_record(marcrecord)
            outputfile.write(marcrecord.as_marc())

outputfile.close()
