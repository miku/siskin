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

Source: 14442
SID: 172
Ticket: #14442

"""


import sys

import marcx

# Default input and output
inputfilename = "172_input.tsv"
outputfilename = "172_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = open(inputfilename, "r")
outputfile = open(outputfilename, "wb")

records = inputfile.readlines()

for line in records[1:]:

    fields = line.split("   ")

    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False

    # Leader
    marcrecord.leader = "     naa  22        4500"

    # ID
    f001 = fields[0]
    marcrecord.add("001", data="finc-172-" + f001)

    # 007
    marcrecord.add("007", data="cr")

    # Sprache
    f041a = fields[10]
    marcrecord.add("041", a=f041a)

    # 1. Schöpfer
    persons = fields[4].split("; ")
    f100a = persons[0]
    marcrecord.add("100", a=f100a)

    # Haupttitel
    f245a = fields[6]
    marcrecord.add("245", a=f245a)

    # Erscheinungsjahr
    f260c = fields[13]
    f260c = f260c.replace("\n", "")
    if len(f260c) == 4:
        marcrecord.add("260", c=f260c)

    # Reihe
    f490a = fields[2]
    marcrecord.add("490", a=f490a)

    # Rechtehinweis
    f500a = fields[3]
    marcrecord.add("500", a=f500a)

    # Dateiformat
    f500a = fields[9]
    marcrecord.add("500", a="Dateiformat: " + f500a)

    # Beschreibung
    f520a = fields[1]
    marcrecord.add("520", a=f520a)

    # Schlagwörter
    f650a = fields[11]
    marcrecord.add("650", a=f650a)

    # weitere Schöpfer
    for person in persons[1:]:
        marcrecord.add("700", a=person)

    # Link zur Ressource
    f856u = fields[12]
    marcrecord.add("856", q="text/html", _3="Link zur Ressource", u=f856u)

    # Medienform
    marcrecord.add("935", b="cofz")

    # Kollektion
    marcrecord.add("980", a=f001, b="172", c="sid-172-col-opal")

    outputfile.write(marcrecord.as_marc())

outputfile.close()
