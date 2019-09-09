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

Source: Kieler Beiträge zur Filmmusikforschung
SID: 101
Ticket: #7967, #9371, #10831, #12048, #12967, #14649, #15882

"""

import io
import re
import sys

import marcx
import base64
import xmltodict

from siskin.utils import check_issn
from siskin.mappings import formats


inputfilename = "101_input.xml"
outputfilename = "101_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = open(inputfilename, "rb")
outputfile = open(outputfilename, "wb")

xmlfile = inputfile.read()
xmlrecords = xmltodict.parse(xmlfile)

format = "Article"
access = "electronic"

"""
<Tabelle1>
<authors>Derlin, Katharina; Niemeier, Patrick</authors>
<rft_x0023_atitle>Rock zwischen Calypso und Twist: Musiker im Rock‘n‘Roll Film, 1956-1963</rft_x0023_atitle>
<rft_x0023_jtitle>Kieler Beiträge zur Filmmusikforschung</rft_x0023_jtitle>
<rft_x0023_pub>Kiel: Kieler Gesellschaft für Filmmusikforschung</rft_x0023_pub>
<rft_x0023_genre>article</rft_x0023_genre>
<rft_x0023_issn>1866-4768</rft_x0023_issn>
<rft_x0023_issue>1</rft_x0023_issue>
<rft_x0023_date>2008</rft_x0023_date>
<rft_x0023_spage>26</rft_x0023_spage>
<rft_x0023_epage>37</rft_x0023_epage>
<rft_x0023_tpages>12</rft_x0023_tpages>
<rft_x0023_pages>26-37</rft_x0023_pages>
<url>http://www.filmmusik.uni-kiel.de/kielerbeitraege/musikimhindifilmTieber.pdf</url>
</Tabelle1>
"""

for xmlrecord in xmlrecords["dataroot"]["Tabelle1"]:

    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False

    # Leader
    leader = formats[format]["Leader"]
    marcrecord.leader = leader

    # Identifikator
    try:
        url = xmlrecord["url"]
    except:
       continue
    url = bytes(url, "utf-8")
    url = base64.b64encode(url)
    f001 = url.decode("utf-8").rstrip("=")
    marcrecord.add("001", data="finc-101-" + f001)

    # Zugang
    if access == "electronic":
        f007 = formats[format]["e007"]
    else:
        f007 = formats[format]["p007"]
    marcrecord.add("007", data=f007)

    # ISSN
    issn = xmlrecord["rft_x0023_issn"]
    f022a = check_issn(issn)
    marcrecord.add("022", a=f022a)

    # 1. Urheber
    authors = xmlrecord.get("authors", "")
    if authors:
        authors = authors.split("; ")
        f100a = authors[0]
        marcrecord.add("100", a=f100a)

    # Titel
    f245 = xmlrecord["rft_x0023_atitle"]
    if ": " in f245:
        f245 = f245.split(": ")
        f245a = f245[0]
        f245b = f245[1]
    else:
        f245a = f245
        f245b = ""
    marcrecord.add("245", a=f245a, b=f245b)

    # Erscheinungsvermerk
    subfields = ("a", "Kiel", "b", "Kieler Gesellschaft für Filmmusikforschung", "c", xmlrecord["rft_x0023_date"])
    marcrecord.add("260", subfields=subfields)

    # weitere Urheber
    if authors:
        if len(authors) > 1:
            for f700a in authors[1:]:
                marcrecord.add("700", a=f700a)

    # Verweis auf Elternelement
    f773t = xmlrecord["rft_x0023_jtitle"]
    pages = xmlrecord["rft_x0023_pages"]
    issue = xmlrecord["rft_x0023_issue"]
    f773g = "Heft: " + issue + " , Seiten: " + pages 
    marcrecord.add("773", t=f773t, g=f773g)

    # Link
    f856u = xmlrecord["url"]
    marcrecord.add("856", q="text/pdf", _3="Link zur Ressource", u=f856u)

    # Ansigelung
    collections = ["a", f001, "b", "101", "c", "sid-101-col-kielfilm"]
    marcrecord.add("980", subfields=collections)

    outputfile.write(marcrecord.as_marc())

outputfile.close()
