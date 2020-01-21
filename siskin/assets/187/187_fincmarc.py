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

Source: UBL / Digitalisierte Zeitschriften (UrMEL)
SID: 187
Ticket: #15951

"""

import argparse
import os
import re
import sys

import xmltodict

import marcx
from siskin.mappings import formats
from siskin.utils import check_isbn, check_issn, marc_build_field_008


def clean_language(language):
    """
    Returns a correct language code for MARC.  
    """
    if language == "de":
        language = "ger"
    elif language == "en":
        language = "eng"
    else:
        print(language)
        sys.exit("check language: " + language)
    return language


# Keyword arguments
parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("-i", dest="inputfile_directory", help="directory of inputfiles", default="187_input", metavar="inputfile_directory")
parser.add_argument("-o", dest="outputfilename", help="outputfile", default="187_output.mrc", metavar="outputfilename")
parser.add_argument("-f", dest="outputformat", help="outputformat marc or marcxml", default="marc", metavar="outputformat")

args = parser.parse_args()
inputfile_directory = args.inputfile_directory
outputfilename = args.outputfilename
outputformat = args.outputformat

if outputformat == "marcxml":
    outputfile = pymarc.XMLWriter(open(outputfilename, "wb"))
else:
    outputfile = open(outputfilename, "wb")

for root, _, files in os.walk(inputfile_directory):

    for inputfilename in files:
        if not inputfilename.endswith(".xml"):
            continue

        inputfilepath = os.path.join(root, inputfilename)
        inputfile = open(inputfilepath, "r", encoding='utf-8')
        xmlfile = inputfile.read()
        xmlrecords = xmltodict.parse(xmlfile, force_list=["dc:creator", "dc:contributor"])

        for xmlrecord in xmlrecords["Records"]["Record"]:

            marcrecord = marcx.Record(force_utf8=True)
            marcrecord.strict = False

            if xmlrecord["header"]["@status"] == "deleted":
                continue

            try:
                setspec = xmlrecord["header"]["setSpec"]
            except:
                continue

            xmlrecord = xmlrecord["metadata"]["oai_dc:dc"]

            title = xmlrecord["dc:title"]
            if title == "Inhalt":
                continue

            # Formatfestlegung
            format = xmlrecord["dc:type"]
            if format == "article":
                format = "Article"
            elif format == "volume":
                format = "Book"
            else:
                sys.exit("Format unbekannt: " + format)

            # Leader
            leader = formats[format]["Leader"]
            marcrecord.leader = leader

            # Identifier
            id = xmlrecord["dc:identifier"][0]
            match1 = re.search("jparticle_(\d+)", id)
            match2 = re.search("jpvolume_(\d+)", id)
            if match1:
                f001 = match1.group(1)
            elif match2:
                f001 = match2.group(1)
            else:
                continue
            marcrecord.add("001", data="187-" + f001)

            # Zugangsfacete
            f007 = formats[format]["e007"]
            marcrecord.add("007", data=f007)

            # Periodizität
            year = xmlrecord["dc:date"]
            periodicity = formats[format]["008"]
            language = xmlrecord["dc:language"]
            language = clean_language(language)
            f008 = marc_build_field_008(year, periodicity, language)
            marcrecord.add("008", data=f008)

            # Sprache
            language = xmlrecord["dc:language"]
            language = clean_language(language)
            marcrecord.add("041", a=language)

            # 1. Urheber
            try:
                persons = xmlrecord["dc:creator"]
            except:
                persons = []
            if persons:
                f100a = persons[0]
                marcrecord.add("100", a=f100a, _4="aut")

            # Haupttitel
            title = xmlrecord["dc:title"]
            if " :: " in title:
                match = re.search("(.*?)\s::\s(.*?)\s:", title)
                if match:
                    f245c, f245a = match.groups()
            elif " / " in title:
                f245 = title.split(" / ")
                f245a = f245[0]
                f245c = f245[-1]
            else:
                f245a = title
                f245c = ""
            marcrecord.add("245", a=f245a, c=f245c)

            # Erscheinungsvermerk
            f260b = xmlrecord["dc:publisher"]
            f260c = xmlrecord["dc:date"]
            marcrecord.add("260", b=f260b, c=f260c)

            # Originalimprint
            title = xmlrecord["dc:title"]
            if " :: " in title:
                match = re.search(".*?\s::\s.*?\s:\s(.*?),\s(.*?),\s(\d\d\d\d)", title)
                if match:
                    f260a, f260b, f260c = match.groups()
                    marcrecord.add("260", a=f260a, b=f260b, c=f260c)

            # Umfangsangabe
            try:
                f300a = xmlrecord["dc:format"]
            except:
                f300a = ""
            if f300a:
                f300a = f300a.replace("SizeOrDuration", "")
                f300a = f300a.strip()
                f300a = f300a.replace(" - ", "-")
                f300a = f300a.replace("*", "")
                f300a = re.sub("-0+", "-", f300a)
                f300a = re.sub("^0+", "", f300a)
                if "-" in f300a:
                    f300a = "S. " + f300a
                marcrecord.add("300", a=f300a)

            # RDA-Inhaltstyp
            f336b = formats[format]["336b"]
            marcrecord.add("336", b=f336b)

            # RDA-Datenträgertyp
            f338b = formats[format]["338b"]
            marcrecord.add("338", b=f338b)

            # Rechtehinweis
            f500a = xmlrecord["dc:rights"]
            marcrecord.add("500", a=f500a)

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

            # übergeordnete Ressource
            if setspec == "jportal_jpjournal_00001103":
                f773t = "Annalen der Naturphilosophie"
                f773w = "572422962"
            elif setspec == "jportal_jpjournal_00001120":
                f773t = "Deutsch als Fremdsprache"
                f773w = "745617522"
            elif setspec == "jportal_jpjournal_00001109":
                f773t = "Jenaische Beyträge zur neuesten gelehrten Geschichte"
                f773w = "756302161"
            elif setspec == "jportal_jpjournal_00001110":
                f773t = "Neue Berichte von Gelehrten Sachen"
                f773w = "756825113"
            elif setspec == "jportal_jpjournal_00001311":
                f773t = "Erneuerte Berichte von gelehrten Sachen"
                f773w = "756825156"
            elif setspec == "jportal_jpjournal_00001014":
                f773t = "Neue Zeitungen von gelehrten Sachen"
                f773w = "728151391"
            elif setspec == "jportal_jpjournal_00001102":
                f773t = "Signale für die musikalische Welt"
                f773w = "756825202"
            elif setspec == "jportal_jpjournal_00001225":
                f773t = "Magazin für Schullehrer..."
                f773w = "820686344"
            else:
                sys.exit("Set unbekannt: " + setspec)
            marcrecord.add("773", t=f773t, w=f773w)

            # Link zur Ressource
            f856u = xmlrecord["dc:identifier"][1]
            if f856u:
                marcrecord.add("856", q="text/html", _3="Link zur Ressource", u=f856u)

            # SWB-Inhaltstyp
            f935c = formats[format]["935c"]
            marcrecord.add("935", c=f935c)

            # Kollektion
            marcrecord.add("980", a=f001, b="187", c="sid-187-col-urmel")

            outputfile.write(marcrecord.as_marc())

    inputfile.close()
outputfile.close()
