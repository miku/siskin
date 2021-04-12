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

from siskin.mappings import formats
from siskin.utils import check_isbn, check_issn, marc_build_field_008

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

    fields = line.split("\t")

    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False

    # Formatfestlegung
    format = "Article"

    # Leader
    leader = formats[format]["Leader"]
    marcrecord.leader = leader

    # ID
    f001 = fields[0]
    f001 = f001.split("-")
    f001 = f001[2]
    marcrecord.add("001", data="finc-172-" + f001)

    # Zugangstyp
    f007 = formats[format]["e007"]
    marcrecord.add("007", data=f007)

    # 008
    language = fields[7]
    if language == "engrish":
        language = "eng"
    elif language == "English":
        language = "eng"
    elif language == "Deutsch":
        language = "ger"
    year = fields[12]
    year = year.replace("\n", "")
    periodicity = formats[format]["008"]
    f008 = marc_build_field_008(year, periodicity, language)
    marcrecord.add("008", data=f008)

    # Sprache
    if len(language) == 3:
        marcrecord.add("041", a=language)

    # 1. Schöpfer
    persons = fields[4]
    if persons:
        num_persons = persons.split(" ")
        if len(num_persons) > 3:
            more_than_one_persons = True
        else:
            more_than_one_persons = False
        if more_than_one_persons and "; " in persons:
            persons = persons.split("; ")
        elif more_than_one_persons and ", " in persons:
            persons = persons.split(", ")
        else:
            persons = persons.split("*")
        f100a = persons[0]
        marcrecord.add("100", a=f100a)

    # Haupttitel
    f245a = fields[1]
    f245b = fields[6]
    if not f245a:
        f245a = fields[6]
        f245b = ""
    if f245a == f245b:
        f245b = ""
    f245c = fields[4]
    marcrecord.add("245", a=f245a, b=f245b, c=f245c)

    # Erscheinungsvermerk
    f260b = fields[8]
    f260c = fields[12]
    f260c = f260c.replace("\n", "")
    if len(f260c) == 4:
        marcrecord.add("260", b=f260b, c=f260c)

    # RDA-Inhaltstyp
    f336b = formats[format]["336b"]
    marcrecord.add("336", b=f336b)

    # RDA-Datenträgertyp
    f338b = formats[format]["338b"]
    marcrecord.add("338", b=f338b)

    # Rechtehinweis
    f500a = fields[3]
    marcrecord.add("500", a=f500a)

    # Beschreibung
    f520a = fields[1]
    marcrecord.add("520", a=f520a)

    # Schlagwörter
    f650a = fields[9]
    marcrecord.add("650", a=f650a)

    # GND-Inhalts- und Datenträgertyp
    f655a = formats[format]["655a"]
    f6552 = formats[format]["6552"]
    marcrecord.add("655", a=f655a, _2=f6552)

    # weitere Schöpfer
    for person in persons[1:]:
        marcrecord.add("700", a=person)

    # Link zur Ressource
    f856u = fields[10]
    marcrecord.add("856", q="text/html", _3="Link zur Ressource", u=f856u)

    # SWB-Inhaltstyp
    f935c = formats[format]["935c"]
    marcrecord.add("935", c=f935c)

    # Kollektion
    marcrecord.add("980", a=f001, b="172", c="sid-172-col-opal")

    outputfile.write(marcrecord.as_marc())

outputfile.close()
