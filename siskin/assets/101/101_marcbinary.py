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
import xlrd

from siskin.mappings import formats
from siskin.utils import check_issn, marc_build_field_008


inputfilename = "101_input.xlsx"
outputfilename = "101_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

outputfile = open(outputfilename, "wb")

workbook = xlrd.open_workbook(inputfilename)
sheet = workbook.sheet_by_name("Tabelle1")

format = "Article"
access = "electronic"

"""
authors = 0
rft_atitle = 1
rft_jtitle = 2 
rft_pub = 3
rft_genre = 4 
rft_issn = 5
rft_issue = 6
rft_volume = 7
rft_date = 8
rft_spage = 9
rft_epage = 10
rft_tpages = 11
rft_pages = 12
url = 13
"""

for row in range(1, sheet.nrows):

    csvrecord = sheet.row_values(row)

    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False

    # Leader
    leader = formats[format]["Leader"]
    marcrecord.leader = leader

    # Identifikator
    try:
        url = csvrecord[13]
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

    # Feld 008
    year = csvrecord[8]
    year = str(year).rstrip("0").rstrip(".") # rstrip(".0") doesn't work equally here
    periodicity = formats[format]["008"]
    language = "ger"
    f008 = marc_build_field_008(year, periodicity, language)
    marcrecord.add("008", data=f008)

    # ISSN
    issn = csvrecord[5]
    issn = str(issn)
    f022a = check_issn(issn)
    marcrecord.add("022", a=f022a)

    # Sprache
    marcrecord.add("041", a="ger")

    # 1. Urheber
    authors = csvrecord[0]
    if authors:
        authors = authors.split("; ")
        f100a = authors[0]
        marcrecord.add("100", a=f100a)

    # Titel
    title = csvrecord[1]
    if ": " in title:
        titles = title.split(": ")
        f245a = titles[0]
        f245b = ""
        for title in titles[1:]:
            f245b = f245b + title + " : "
    else:
        f245a = title
        f245b = ""
    f245a = f245a.strip()
    f245b = f245b.rstrip(" :")
    f245b = f245b.strip()
    marcrecord.add("245", a=f245a, b=f245b)

    # Erscheinungsvermerk
    year = csvrecord[8]
    f260c = str(year).rstrip("0").rstrip(".") # rstrip(".0") doesn't work equally here
    subfields = ("a", "Kiel", "b", "Kieler Gesellschaft für Filmmusikforschung", "c", f260c)
    marcrecord.add("260", subfields=subfields)

    # Umfang
    pages = str(csvrecord[12])
    extent = pages.split("-")
    if len(extent) == 2:
        start = int(extent[0])
        end = int(extent[1])
        f300a = str(end - start + 1) + " Seiten"
    else:
        f300a = ""
    marcrecord.add("300", a=f300a)

    # weitere Urheber
    if authors:
        if len(authors) > 1:
            for f700a in authors[1:]:
                marcrecord.add("700", a=f700a)

    # Verweis auf Elternelement
    f773t = csvrecord[2]
    year = str(csvrecord[8]).rstrip(".0")
    pages = str(csvrecord[12])
    issue = str(csvrecord[6]).rstrip(".0")
    f773g = "(" + year + "), Heft: " + issue + ", S. " + pages 
    marcrecord.add("773", t=f773t, g=f773g)

    # Link
    f856u = csvrecord[13]
    marcrecord.add("856", q="text/pdf", _3="Link zur Ressource", u=f856u)

    # Ansigelung
    collections = ["a", f001, "b", "101", "c", "sid-101-col-kielfilm"]
    marcrecord.add("980", subfields=collections)
    outputfile.write(marcrecord.as_marc())

outputfile.close()
