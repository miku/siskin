#!/usr/bin/env python
# coding: utf-8
#
# Copyright 2019 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
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

Source: Umweltbibliothek Leipzig
SID: 156
Ticket: #11786

"""

import re
import sys

import marcx
import pymarc
from siskin.mappings import formats
from siskin.utils import marc_clean_record, check_isbn, check_issn

copytags = ("100", "105", "120", "130", "150", "174", "200", "245", "246", "250", "260", "300",
            "335", "351", "361", "400", "500", "520", "650", "689", "700", "710", "800")

inputfilename = "156_input.xml"
outputfilename = "156_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = open(inputfilename, "rb")
outputfile = open(outputfilename, "wb")
oldrecords = pymarc.parse_xml_to_array(inputfile)

for i, oldrecord in enumerate(oldrecords, start=1):

    try:
        f245a = oldrecord["245"]["a"]
    except:
        continue

    newrecord = marcx.Record(force_utf8=True)
    newrecord.strict = False

    # pauschale Festlegung
    format = "Book"

    # leader
    leader = formats[format]["Leader"]
    newrecord.leader = leader

    # Identifikator
    f001 = str(i)
    newrecord.add("001", data="finc-156-" + f001)

    # Zugangsart
    f007 = formats[format]["p007"]
    newrecord.add("007", data=f007)

    # ISBN
    try:
        isbn = oldrecord["020"]["a"]
    except:
        isbn = ""
    f020a = check_isbn(isbn)
    newrecord.add("020", a=f020a)

    # ISSN
    try:
        issn = oldrecord["022"]["a"]
    except:
        issn = ""
    f022a = check_issn(issn)
    newrecord.add("022", a=f022a)

    # RDA-Inhaltstyp
    f336b = formats[format]["336b"]
    newrecord.add("336", b=f336b)

    # RDA-Datenträgertyp
    f338b = formats[format]["338b"]
    newrecord.add("338", b=f338b)

    # Originalfelder, die ohne Änderung übernommen werden
    for tag in copytags:
        for field in oldrecord.get_fields(tag):
            newrecord.add_field(field)

    # 980
    collections = ["a", f001, "b", "156", "c", "sid-156-col-umweltbibliothek"]
    newrecord.add("980", subfields=collections)

    marc_clean_record(newrecord)
    outputfile.write(newrecord.as_marc())

inputfile.close()
outputfile.close()
