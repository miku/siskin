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

Source: Abendländische mittelalterliche Handschriften
SID: 159
Ticket: #13316, #14527, #15488, #16052

"""

import re
import sys

import marcx
import pymarc
from siskin.utils import marc_clean_record


inputfilename = "159_input.xml"
outputfilename = "159_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = open(inputfilename, "rb")
outputfile = open(outputfilename, "wb")
reader = pymarc.parse_xml_to_array(inputfile)

for record in reader:

    record = marcx.Record.from_record(record)
    record.force_utf8 = True
    record.strict = False

    # Identifikator
    f001 = record["001"].data
    f001 = f001.replace("-", "")
    f001 = f001.replace(",T", "T")
    record.remove_fields("001")
    record.add("001", data="159-" + f001)

    # Titel
    f245p = record["245"]["b"]
    record.remove_fields("245")
    record.add("245", a="Abendländische mittelalterliche Handschriften", p=f245p)

    # weitere Urheber
    unique_persons = []
    fields = record.get_fields("700")
    record.remove_fields("700")
    if fields:
        for field in fields:
            person = field.get_subfields("a")[0]
            gnd = field.get_subfields("1")
            if person and gnd:
                gnd = gnd[0]
                if gnd not in unique_persons:
                    record.add("700", a=person, _1=gnd)
                    unique_persons.append(person)
                    unique_persons.append(gnd)
            elif person:
                if person not in unique_persons:
                    record.add("700", a=person)
                    unique_persons.append(person)
    
    # Elternelemente
    record.add("773", w="sid-159-col-buchhsabendlandmittelalter")

    # Ansigelung
    try:
        f912a = record["912"]["a"]
    except:
        print(f001)
        continue
    if f912a == "Abendländische mittelalterliche Handschriften" or f912a == "abendländische mittelalterliche Handschriften":
        f980c = "sid-159-col-buchhsabendlandmittelalter"
    elif f912a == "Abendländische neuzeitliche Handschriften":
        f980a = "id-159-col-buchhsabendlandneuzeit"
    #elif f912a == "Andere Buchhandschriften":
    #    f980a = ""
    elif f912a == "Fragmente":
        f980a = "sid-159-col-buchhsfragmente"
    elif f912a == "Griechische Handschriften":
        f980a = "sid-159-col-buchhsgriechisch"
    elif f912a == "Neuzeitliche Handschriften":
        f980a = "id-159-col-buchhsabendlandneuzeit"
    elif f912a == "Hebräische Handschriften":
        f980a = "sid-159-col-buchhshebraeisch"
    record.add("980", a=f001, b="159", c=f980c)

    marc_clean_record(record)
    outputfile.write(record.as_marc())

inputfile.close()
outputfile.close()
