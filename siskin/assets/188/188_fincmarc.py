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

Source: GoeScholar - Publikationenserver der Georg-August-Universität Göttingen
SID: 188
Ticket: #16357

"""

import argparse
import os
import re
import sys

import xmltodict

import marcx
import pymarc
from siskin.mappings import formats
from siskin.utils import check_isbn, check_issn, marc_build_field_008


def clean_language(language):
    """
    Returns a correct language code for MARC.  
    """
    if language == "deu" or language == "de":
        language = "ger"

    if language != "ger" and language != "eng" and language != "":
        sys.exit("check language: " + language)
    return language


def get_year(xmlrecord):
    """
    Takes all fields dc:date and returns a proper four-digit year.
    """
    dates = xmlrecord["dc:date"]
    for date in dates:
        match = re.match("(\d\d\d\d)", date)
        if match:
            year = match.group(1)
            return year
    else:
        return ""


# Keyword arguments
parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("-i", dest="inputfilename", help="inputfile", default="188_input.xml", metavar="inputfilename")
parser.add_argument("-o", dest="outputfilename", help="outputfile", default="188_output.mrc", metavar="outputfilename")
parser.add_argument("-f", dest="outputformat", help="outputformat marc or marcxml", default="marc", metavar="outputformat")

args = parser.parse_args()
inputfilename = args.inputfilename
outputfilename = args.outputfilename
outputformat = args.outputformat

if outputformat == "marcxml":
    outputfile = pymarc.XMLWriter(open(outputfilename, "wb"))
else:
    outputfile = open(outputfilename, "wb")

inputfile = open(inputfilename, "r", encoding='utf-8')
xmlfile = inputfile.read()
xmlrecords = xmltodict.parse(
    xmlfile,
    force_list=["setSpec", "dc:identifier", "dc:creator", "dc:contributor", "dc:type", "dc:language", "dc:subject", "dc:publisher", "dc:date", "dc:title"])

for xmlrecord in xmlrecords["Records"]["Record"]:

    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False

    if xmlrecord["header"]["@status"] == "deleted":
        continue

    if "col_1_11730" not in xmlrecord["header"]["setSpec"] and "col_goescholar_3015" not in xmlrecord["header"]["setSpec"]:
        continue

    xmlrecord = xmlrecord["metadata"]["oai_dc:dc"]

    # Formatfestlegung
    types = xmlrecord["dc:type"]
    if "anthology" in types:
        format = "Book"
    elif "journalArticle" in types:
        format = "Article"
    elif "anthologyArticle" in types:
        format = "Article"
    elif "monograph" in types:
        format = "Book"
    else:
        #sys.exit("Format unbekannt: " + str(types))
        format = "Article"

    # Leader
    leader = formats[format]["Leader"]
    marcrecord.leader = leader

    # Identifier
    ids = xmlrecord["dc:identifier"]
    for id in ids:
        if "http" in id:
            match = re.search(".*\/(\d+)", id)
            if match:
                f001 = match.group(1)
                marcrecord.add("001", data="188-" + f001)
                break
    else:
        continue

    # Zugangsfacete
    f007 = formats[format]["e007"]
    marcrecord.add("007", data=f007)

    # Periodizität
    year = get_year(xmlrecord)
    periodicity = formats[format]["008"]
    try:
        language = xmlrecord["dc:language"][0]
    except:
        language = ""
    language = clean_language(language)
    f008 = marc_build_field_008(year, periodicity, language)
    marcrecord.add("008", data=f008)

    # ISSN
    try:
        issns = xmlrecord["dc:relation"]
    except:
        issns = []
    for issn in issns:
        issn = check_issn(issn)
        marcrecord.add("022", a=issn)

    # Sprache
    try:
        languages = xmlrecord["dc:language"]
    except:
        languages = []
    for language in languages:
        language = clean_language(language)
        marcrecord.add("041", a=language)

    # DDC-Notation
    subjects = xmlrecord["dc:subject"]
    for subject in subjects:
        match = re.match("\d+", subject)
        if match:
            marcrecord.add("082", a=subject)
            break

    # 1. Urheber
    try:
        persons = xmlrecord["dc:creator"]
    except:
        persons = []
    if persons:
        f100a = persons[0]
        marcrecord.add("100", a=f100a, _4="aut")

    # Haupttitel
    title = xmlrecord["dc:title"][0]
    if ":" in title:
        title = title.split(":")
        f245a = title[0]
        f245b = str(title[1:])
        f245a = f245a.strip()
        f245b = f245b.strip()
    else:
        f245a = title
        f245b = ""
    try:
        f245b = xmlrecord["dc:title"][1]
    except:
        f245b = ""
    marcrecord.add("245", a=f245a, b=f245b)

    # Erscheinungsvermerk
    try:
        f260 = xmlrecord["dc:publisher"]
    except:
        f260 = ""

    if f260:

        f260a = ""
        f260b = ""

        if len(xmlrecord["dc:publisher"]) == 2:
            f260a = xmlrecord["dc:publisher"][1]
            f260b = xmlrecord["dc:publisher"][0]
        else:
            f260a = xmlrecord["dc:publisher"][0]

    f260c = get_year(xmlrecord)
    marcrecord.add("260", a=f260a, b=f260b, c=f260c)

    # RDA-Inhaltstyp
    f336b = formats[format]["336b"]
    marcrecord.add("336", b=f336b)

    # RDA-Datenträgertyp
    f338b = formats[format]["338b"]
    marcrecord.add("338", b=f338b)

    # Rechtehinweis
    try:
        f500a = xmlrecord["dc:rights"]
    except:
        f500a = ""
    marcrecord.add("500", a=f500a)

    # Schlagwörter
    dcsubjects = xmlrecord["dc:subject"]
    for dcsubject in dcsubjects:
        match = re.match("\d+", dcsubject)
        if not match:
            subjects = dcsubject.split(";")
            for subject in subjects:
                f650a = subject.strip()
                f650a = f650a.title()
                marcrecord.add("650", a=f650a)

    # GND-Inhalts- und Datenträgertyp
    f655a = formats[format]["655a"]
    f6552 = formats[format]["6552"]
    marcrecord.add("655", a=f655a, _2=f6552)

    # weitere Urheber
    for f700a in persons[1:]:
        marcrecord.add("700", a=f700a, _4="aut")

    # Herausgeber und sonstige Beteiligte
    try:
        editors = xmlrecord["dc:contributor"]
    except:
        editors = []
    if editors:
        for f700a in editors:
            marcrecord.add("700", a=f700a, _4="edt")

    # Link zur Ressource
    f856u = xmlrecord["dc:identifier"][0]
    if f856u:
        marcrecord.add("856", q="text/html", _3="Link zur Ressource", u=f856u)

    # SWB-Inhaltstyp
    f935c = formats[format]["935c"]
    marcrecord.add("935", c=f935c)

    # Kollektion
    marcrecord.add("980", a=f001, b="188", c="sid-188-col-goescholar")

    # MARC-Datensatz in Datei schreiben
    if outputformat == "marcxml":
        outputfile.write(marcrecord)
    else:
        outputfile.write(marcrecord.as_marc())

inputfile.close()
outputfile.close()
