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

Source: British Library Catalogue
SID: 181
Ticket: #15835

"""

import sys

import marcx
import pymarc
from siskin.utils import marc_clean_record, xmlstream

inputfilename = "181_input.mrc"
outputfilename = "181_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = open(inputfilename, "rb")
outputfile = open(outputfilename, "wb")
reader = pymarc.MARCReader(inputfile, force_utf8=True)

for record in reader:

    record = marcx.Record.from_record(record)
    record.force_utf8 = True
    record.strict = False

    # Identifikator
    f001 = record["001"].data
    record.remove_fields("001")
    record.add("001", data="181-" + f001)

    # Zugangsart
    try:
        f007 = record["007"].data
    except:
        record.add("007", data="tu")

    # Link zum EThOS-Datensatz
    try:
        f852b = record["852"]["b"]
    except:
        f852b = ""

    try:
        f852c = record["852"]["c"]
    except:
        f852c = ""

    try:
        f852j = record["852"]["j"]
    except:
        f852j = ""

    if f852b == "DSC" and f852c == "DRT" and f852j:
        f856u = "https://ethos.bl.uk/OrderDetails.do?uin=uk.bl.ethos." + f852j
        record.add("856", q="text/html", _3="Link zur Ressource", u=f856u)

    collections = ["a", f001, "b", "181", "c", "sid-181-col-blfidbbi"]
    record.add("980", subfields=collections)

    marc_clean_record(record)
    outputfile.write(record.as_marc())

outputfile.close()
