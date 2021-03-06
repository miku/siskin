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
import re
import sys
from io import BytesIO, StringIO

import requests

import marcx
import pymarc
from siskin.utils import marc_clean_record
from six.moves.urllib.parse import urlencode


def get_sru_records(start, max):

    params = {
        "maximumRecords":
        max,
        "startRecord":
        start,
        "recordSchema":
        "marcxml",
        "operation":
        "searchRetrieve",
        "query":
        'pica.bkl="05.15" or pica.bkl="05.30" or pica.bkl="05.33" or pica.bkl="05.38" or pica.bkl="05.39" or pica.bkl="06.*" or pica.bkl="17.80" or pica.bkl="17.91" or pica.bkl="17.94" or pica.bkl="21.30" or pica.bkl="21.31" or pica.bkl="21.32" or pica.bkl="21.93" or pica.bkl="53.70" or pica.bkl="53.71" or pica.bkl="53.72" or pica.bkl="53.73" or pica.bkl="53.74" or pica.bkl="53.75" or pica.bkl="53.76" or pica.bkl="53.82" or pica.bkl="53.89" or pica.bkl="54.08" or pica.bkl="54.10" or pica.bkl="54.22" or pica.bkl="54.32" or pica.bkl="54.38" or pica.bkl="54.51" or pica.bkl="54.52" or pica.bkl="54.53" or pica.bkl="54.54" or pica.bkl="54.55" or pica.bkl="54.62" or pica.bkl="54.64" or pica.bkl="54.65" or pica.bkl="54.72" or pica.bkl="54.73" or pica.bkl="54.74" or pica.bkl="54.75" or pica.bkl="86.28" or pica.rvk="AM 1*" or pica.rvk="AM 2*" or pica.rvk="AM 3*" or pica.rvk="AM 4*" or pica.rvk="AM 5*" or pica.rvk="AM 6*" or pica.rvk="AM 7*" or pica.rvk="AM 8*" or pica.rvk="AM 9*" or pica.rvk="AN 1*" or pica.rvk="AN 2*" or pica.rvk="AN 3*" or pica.rvk="AN 4*" or pica.rvk="AN 5*" or pica.rvk="AN 6*" or pica.rvk="AN 7*" or pica.rvk="AN 8*" or pica.rvk="AN 9*" or pica.rvk="AP 25*" or pica.rvk="AP 28*" or pica.rvk="LH 72*" or pica.rvk="LK 86*" or pica.rvk="LR 5570*" or pica.rvk="LR 5580*" or pica.rvk="LR 5581*" or pica.rvk="LR 5582*" or pica.rvk="LR 5586*" or pica.rvk="LR 57240" or pica.rvk="LT 5570*" or pica.rvk="LT 5580*" or pica.rvk="LT 5581*" or pica.rvk="LT 5582*" or pica.rvk="LT 5586*" or pica.rvk="LT 57240" or pica.ssg="bbi" or pica.ssg="24,1" or pica.ssg="bub"'
    }

    params = urlencode(params)
    result = requests.get("https://sru.k10plus.de/k10plus?%s" % params)
    result = result.text
    result = result.replace("\n", "")
    result = result.replace("</record>", "</record>\n")
    return result.split("\n")


outputfilename = "183_output.xml"

if len(sys.argv) == 2:
    outputfilename = sys.argv[1:]

writer = pymarc.XMLWriter(open(outputfilename, "wb"))

result = get_sru_records(1, 1)
n = result[0]
match = re.search("\<zs:numberOfRecords\>(.*?)\</zs:numberOfRecords\>", n)
if match:
    all_records = match.group(1)
else:
    sys.exit("could get numberOfRecords")

all_records = int(all_records)
max_records_request = 100
start = 1

while start < all_records:

    oldrecords = get_sru_records(start, max_records_request)

    if not oldrecords:
        sys.exit("Anfrage nicht erfolgreich")

    if len(oldrecords) == 0:
        sys.exit("leere Liste")

    for oldrecord in oldrecords[:-1]:

        match = re.search("(\<record.*?\>.*?\</record\>)", oldrecord)  #327963263

        if not match:
            sys.exit("kein record-Element")

        oldrecord = match.group(1)
        oldrecord = oldrecord.encode("latin-1")
        oldrecord = BytesIO(oldrecord)
        oldrecord = pymarc.marcxml.parse_xml_to_array(oldrecord)
        oldrecord = oldrecord[0]

        newrecord = marcx.Record.from_record(oldrecord)
        newrecord.force_utf8 = True
        newrecord.strict = False

        try:
            newrecord["245"]["a"]
        except:
            continue

        # Identifikator
        f001 = newrecord["001"].data
        newrecord.remove_fields("001")
        newrecord.add("001", data="183-" + f001)

        # Ansigelung und Kollektion
        collections = ["a", f001, "b", "183", "c", "sid-183-col-kxpbbi"]
        newrecord.add("980", subfields=collections)

        marc_clean_record(newrecord)
        writer.write(newrecord)

    start += max_records_request

writer.close()
