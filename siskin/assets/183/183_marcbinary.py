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

import re
import sys

import xmltodict

import marcx
from siskin.mappings import formats, roles
from siskin.utils import (check_isbn, check_issn, marc_build_field_008, xmlstream)


def get_field(record, tag, code, all=False):
    """
    Iterates over fields and subfields and returns the desired value.
    """
    values = []
    for field in record["ns0:record"]["ns0:datafield"]:
        if field["@tag"] != tag:
            continue
        if isinstance(field["ns0:subfield"], list):
            for subfield in field["ns0:subfield"]:
                if subfield["@code"] != code:
                    continue
                if not all:
                    return subfield["#text"]
                values.append(subfield["#text"])
        else:
            if field["ns0:subfield"]["@code"] == code:
                if not all:
                    return field["ns0:subfield"]["#text"]
                values.append(field["ns0:subfield"]["#text"])
    return values


inputfilename = "183_input.xml"
outputfilename = "183_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

outputfile = open(outputfilename, "wb")

for oldrecord in xmlstream(inputfilename, "record"):

    oldrecord = xmltodict.parse(oldrecord)
    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False

    # Formatzuweisung
    f002c = get_field(oldrecord, "002C", "a")
    f002d = get_field(oldrecord, "002D", "a")
    f002e = get_field(oldrecord, "002E", "a")
    f037c = get_field(oldrecord, "037C", "d")
    
    if not f002c or not f002d or not f002e:
        continue

    if f037c:
        format = "Thesis"
    elif f002c == "Text" and f002d == "ohne Hilfsmittel zu benutzen" and f002e == "Band":
        format = "Book"
    elif f002c == "Text" and f002d == "Band" and f002e == "ohne Hilfsmittel zu benutzen":
        format = "Book"
    elif f002c == "Text" and f002d == "ohne Hilfsmittel zu benutzen" and f002e == "Blatt":
        format = "Book"
    elif f002c == "Text" and f002d == "Computermedien" and f002e == "Online-Ressource":
        format = "Website"
    elif f002c == "Text" and f002d == "Computermedien" and f002e == "Computerdisk":
        format = "CD-ROM"
    elif f002c == "Text" and f002d == "Computermedien" and f002e == "Computerchip-Cartridge":
        format = "CD-ROM"
    elif f002c == "unbewegtes Bild" and f002d == "ohne Hilfsmittel zu benutzen" and f002e == "Band":
        format = "Book"
    elif f002c == "Sonstige" and f002d == "ohne Hilfsmittel zu benutzen" and f002e == "Band":
        format = "Book"
    elif f002c == "nicht spezifiziert" and f002d == "ohne Hilfsmittel zu benutzen" and f002e == "Band":
        format = "Book"
    elif f002c == "zweidimensionales bewegtes Bild" and f002d == "video" and f002e == "Videodisk":
        format = "DVD-Video"
    elif f002c == "Noten" and f002d == "Computermedien" and f002e == "Online-Ressource":
        format = "Website"
    elif f002c == "Noten" and f002d == "ohne Hilfsmittel zu benutzen" and f002e == "Band":
        format = "Score"
    elif f002c == "unbewegtes Bild" and f002d == "ohne Hilfsmittel zu benutzen" and f002e == "Blatt":
        format = "Image"
    elif f002c == "Computerdaten" and f002d == "Computermedien" and f002e == "Computerdisk":
        format = "CD-ROM"
    elif f002c == "Computerprogramm" and f002d == "Computermedien" and f002e == "Computerdisk":
        format = "CD-ROM"
    elif f002c == "Text" and f002d == "Computermedien" and f002e == "Band":
        format = "Book"
    elif f002c == "unbewegtes Bild" and f002d == "Computermedien" and f002e == "Online-Ressource":
        format = "Website"
    elif f002c == "kartografisches Bild" and f002d == "ohne Hilfsmittel zu benutzen" and f002e == "Band":
        format = "Book"
    elif f002c == "kartografisches Bild" and f002d == "ohne Hilfsmittel zu benutzen" and f002e == "Blatt":
        format = "Map"
    elif f002c == "gesprochenes Wort" and f002d == "audio" and f002e == "Audiodisk":
        format = "Audio-CD"
    else:
        format = "Book"

    # Leader
    leader = formats[format]["Leader"]
    marcrecord.leader = leader

    # Identifikator
    f001 = get_field(oldrecord, "003@", "0")
    marcrecord.add("001", data="183-" + f001)
    
    # Zugangsart
    f002e = get_field(oldrecord, "002E", "b")
    if f002e != "c" and f002e != "cr":
        f007 = formats[format]["p007"]
    else:
        f007 = formats[format]["e007"]
    marcrecord.add("007", data=f007)

    # Periodizität
    year = get_field(oldrecord, "011@", "a")
    periodicity = formats[format]["008"]
    language = get_field(oldrecord, "010@", "a")
    f008 = marc_build_field_008(year, periodicity, language)
    marcrecord.add("008", data=f008)

    # ISBN
    isbns = get_field(oldrecord, "004A", "0", all=True)
    for isbn in isbns:
        f020a = check_isbn(isbn)
        marcrecord.add("020", a=f020a)

    # ISSN
    issns = get_field(oldrecord, "005A", "0", all=True) 
    for issn in issns:
        f022a = check_isbn(isbn)
        marcrecord.add("022", a=f022a)

    # Sprache
    f041a = get_field(oldrecord, "001@", "a")
    marcrecord.add("041", a=f041a)  

    # 1. Schöpfer
    forename = get_field(oldrecord, "028A", "d")
    surname = get_field(oldrecord, "028A", "a")
    if surname:
        f100a = surname + ", " + forename
        f100d = get_field(oldrecord, "028A", "B")
        f1004 = get_field(oldrecord, "028A", "4")
        marcrecord.add("100", a=f100a, d=f100d, _4=f1004)

    # Haupttitel, Titelzusatz und Verantwortliche
    f245a = get_field(oldrecord, "021A", "a")
    f245b = get_field(oldrecord, "021A", "b")
    f245c = get_field(oldrecord, "021A", "h")
    subfields = ["a", f245a, "b", f245b, "c", f245c]
    marcrecord.add("245", subfields=subfields)

    # Ausgabevermerk
    f250a = get_field(oldrecord, "032@", "a")
    marcrecord.add("250", a=f250a)  

    # Erscheinungsvermerk
    f260a = get_field(oldrecord, "033A", "p")
    f260b = get_field(oldrecord, "033A", "n")
    f260c = get_field(oldrecord, "011@", "a")
    subfields = ["a", f260a, "b", f260b, "c", f260c]
    marcrecord.add("260", subfields=subfields)

    # Umfangsangabe
    f300a = get_field(oldrecord, "034D", "a")
    f300b = get_field(oldrecord, "034M", "a")
    f300c = get_field(oldrecord, "034I", "a")
    marcrecord.add("300", a=f300a, b=f300b, c=f300c)

    # RDA-Inhaltstyp
    f336b = formats[format]["336b"]
    marcrecord.add("336", b=f336b)

    # RDA-Datenträgertyp
    f338b = formats[format]["338b"]
    marcrecord.add("338", b=f338b)

    # Reihe
    f490a = get_field(oldrecord, "036E", "a")
    f490n = get_field(oldrecord, "036E", "l")
    marcrecord.add("490", a=f490a, n=f490n)

    # Schlagwort
    subjects = get_field(oldrecord, "041A", "a", all=True)
    for f650a in subjects:
        marcrecord.add("650", a=f650a)

    # GND-Inhalts- und Datenträgertyp
    f655a = formats[format]["655a"]
    f6552 = formats[format]["6552"]
    marcrecord.add("655", a=f655a, _2=f6552)

    # weitere geistige Schöpfer
    persons = get_field(oldrecord, "028B", "a", all=True)
    num = len(persons)
    for i in range(0, num):
        forename = get_field(oldrecord, "028B", "d", all=True)[i]
        surname = get_field(oldrecord, "028B", "a", all=True)[i]
        f700a = surname + ", " + forename
        try:
            f700d = get_field(oldrecord, "028B", "B", all=True)[i]
        except:
            f700d = ""
        try:
            f7004 = get_field(oldrecord, "028B", "4", all=True)[i]
        except:
            f7004 = ""
        marcrecord.add("700", a=f700a, d=f700d, _4=f7004)
    
    # Link zur Ressource
    f856u = get_field(oldrecord, "009Q", "a")
    if f856u:
        marcrecord.add("856", q="image/jpeg", _3="Link zum Digitalisat", u=f856u)

    # SWB-Inhaltstyp
    f935c = formats[format]["935c"]
    marcrecord.add("935", c=f935c)

    # Ansigelung und Kollektion
    collections = ["a", f001, "b", "183", "c", "sid-183-col-kxpbbi"]
    marcrecord.add("980", subfields=collections)

    outputfile.write(marcrecord.as_marc())

outputfile.close()
