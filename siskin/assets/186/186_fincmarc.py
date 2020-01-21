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

Source: Datenbank Buch und Papier
SID: 186
Ticket: #16115

"""

import argparse
import io
import re
import sys

import xmltodict

import marcx
import pymarc
from siskin.mappings import formats
from siskin.utils import (check_isbn, check_issn, marc_build_field_008, marc_build_field_773g)


def get_field(xmlrecord, tag):
    # print(xmlrecord)
    for field in xmlrecord['field']:
        if field["@tag"] != tag:
            continue
        else:
            # because of such occurrences: <field tag="77"/>
            try:
                field = field["#text"]
            except:
                field = ""
            return field
    return ""


# Keyword arguments
parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("-i", dest="inputfilename", help="inputfile", default="186_input.xml", metavar="inputfilename")
parser.add_argument("-o", dest="outputfilename", help="outputfile", default="186_output.mrc", metavar="outputfilename")
parser.add_argument("-f", dest="outputformat", help="outputformat marc or marcxml", default="marc", metavar="outputformat")

args = parser.parse_args()
inputfilename = args.inputfilename
outputfilename = args.outputfilename
outputformat = args.outputformat

inputfile = open(inputfilename, "r")

if outputformat == "marcxml":
    outputfile = pymarc.XMLWriter(open(outputfilename, "wb"))
else:
    outputfile = open(outputfilename, "wb")

xmlfile = inputfile.read()
xmlrecords = xmltodict.parse(xmlfile, force_list=('field', ))

for xmlrecord in xmlrecords["collection"]["record"]:

    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False

    # Formatfestelegung
    journal = get_field(xmlrecord, "78")
    if journal:
        format = "Article"
    else:
        format = "Book"

    # Leader
    leader = formats[format]["Leader"]
    marcrecord.leader = leader

    # Identifier
    f001 = get_field(xmlrecord, "10")
    marcrecord.add("001", data="186-" + f001)

    # Zugangsfacette
    f007 = formats[format]["p007"]
    marcrecord.add("007", data=f007)

    # Periodizität
    year = get_field(xmlrecord, "76")
    periodicity = formats[format]["008"]
    language = ""
    f008 = marc_build_field_008(year, periodicity, language)
    marcrecord.add("008", data=f008)

    # ISBN
    f020a = get_field(xmlrecord, "87")
    f020a = check_isbn(f020a)
    marcrecord.add("020", a=f020a)

    # ISSN
    f022a = get_field(xmlrecord, "88")
    f022a = check_isbn(f022a)
    marcrecord.add("022", a=f022a)

    # Notation
    f084a = get_field(xmlrecord, "30")
    marcrecord.add("084", a=f084a)

    # 1. Urheber (Person)
    f100a = get_field(xmlrecord, "40")
    marcrecord.add("100", a=f100a)

    # 1. Urheber (Körperschaft)
    f110a = get_field(xmlrecord, "60")
    marcrecord.add("110", a=f110a)

    # Haupttitel und Verantwortlichkeitsangabe
    f245 = get_field(xmlrecord, "20")
    f245 = f245.replace("¬", "")
    f245 = f245.replace("\n", "")
    f245 = f245.replace("   ", "")
    if ":" in f245:
        f245 = f245.split(":")
        f245a = f245[0].strip()
        f245b = f245[1].strip()
    else:
        f245a = f245
        f245b = ""
    f245c = get_field(xmlrecord, "39")
    marcrecord.add("245", a=f245a, b=f245b, c=f245c)

    # Alternativer Titel
    f246a = get_field(xmlrecord, "24")
    f246a = f246a.replace("\n", "")
    marcrecord.add("246", a=f246a)

    # Erscheinungsvermerk
    f260a = get_field(xmlrecord, "74")
    f260b = get_field(xmlrecord, "75")
    f260c = get_field(xmlrecord, "76")
    subfields = ("a", f260a, "b", f260b, "c", f260c)
    marcrecord.add("260", subfields=subfields)

    # Umfangsangabe
    f300a = get_field(xmlrecord, "77")
    match = re.search("(\d+)-(\d+)", f300a)
    if match:
        spage, epage = match.groups()
        f300a = int(epage) - int(spage) + 1
        f300a = str(f300a) + " S."
    marcrecord.add("300", a=f300a)

    # RDA-Inhaltstyp
    f336b = formats[format]["336b"]
    marcrecord.add("336", b=f336b)

    # RDA-Datenträgertyp
    f338b = formats[format]["338b"]
    marcrecord.add("338", b=f338b)

    # Reihe
    f490 = get_field(xmlrecord, "85")
    match = re.search("(.*?) ; (\d+)", f490)
    if match:
        f490a, f490v = match.groups()
    else:
        f490a = f490
        f490v = ""
    marcrecord.add("490", a=f490a, v=f490v)

    # Fußnote
    f500a = get_field(xmlrecord, "81")
    marcrecord.add("500", a=f500a)

    # Schlagwörter
    f650 = []
    tags = ["31", "312", "313", "314", "315"]
    for tag in tags:
        subjects = get_field(xmlrecord, tag)
        subjects = subjects.split(" : ")
        for subject in subjects:
            subject = subject.replace("\n", "")
            subject = subject.replace("   ", "")
            f650.append(subject)
    f650 = set(f650)
    for f650a in f650:
        marcrecord.add("650", a=f650a)

    # GND-Inhalts- und Datenträgertyp
    f655a = formats[format]["655a"]
    f6552 = formats[format]["6552"]
    marcrecord.add("655", a=f655a, _2=f6552)

    # weitere Personen
    for i in range(41, 59):
        tag = str(i)
        f700a = get_field(xmlrecord, tag)
        marcrecord.add("700", a=f700a)

    # 1. Körperschaften
    f710a = get_field(xmlrecord, "61")
    marcrecord.add("710", a=f710a)

    # Zeitschrift
    if format == "Article":
        f773t = get_field(xmlrecord, "78")
        f773t = f773t.replace("¬", "")
        volume = get_field(xmlrecord, "72")
        year = get_field(xmlrecord, "76")
        issue = get_field(xmlrecord, "73")
        pages = get_field(xmlrecord, "77")
        match = re.search("(\d+)-(\d+)", pages)
        if match:
            spage, epage = match.groups()
        else:
            spage = ""
            epage = ""
        f773g = marc_build_field_773g(volume, year, issue, spage, epage)
        marcrecord.add("773", t=f773t, g=f773g)

    # SWB-Inhaltstyp
    f935c = formats[format]["935c"]
    marcrecord.add("935", c=f935c)

    marcrecord.add("980", a=f001, b="186", c="sid-186-col-bup")

    outputfile.write(marcrecord.as_marc())

inputfile.close()
outputfile.close()
