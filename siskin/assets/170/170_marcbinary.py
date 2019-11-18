#!/usr/bin/env python3
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

Source: mediarep
SID: 170
Ticket: #14064, #15766, #16137

"""

import io
import re
import sys

import xmltodict

import marcx
from siskin.mappings import formats
from siskin.utils import check_isbn, check_issn, marc_build_field_008


def delistify(value, first=True, concat=None):
    """
    Returns a string value. If value is a list, choose a strategy to create a
    single value.
    """
    if isinstance(value, (list, tuple)):
        if len(value) > 0:
            if first:
                return value[0]
            if concat is not None:
                return concat.join(value)
        else:
            return ''
    return value


inputfilename = "170_input.xml"
outputfilename = "170_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = open(inputfilename, "rb")
outputfile = open(outputfilename, "wb")

xmlfile = inputfile.read()
xmlrecords = xmltodict.parse(xmlfile)

for xmlrecord in xmlrecords["Records"]["Record"]:

    if not xmlrecord["metadata"]["oai_dc:dc"].get("dc:title"):
        continue

    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False

    # Formatmerkennung
    allformats = xmlrecord["metadata"]["oai_dc:dc"]["dc:type"]
    for format in allformats:
        if format == "book":
            format = "Book"
            break
        elif format == "bookPart":
            format = "Chapter"
            break
        elif format == "doctoralThesis":
            format = "Thesis"
            break
        elif format == "MovingImage":
            format = "Online-Video"
            break
    else:
        format = "Article"

    # Leader
    leader = formats[format]["Leader"]
    marcrecord.leader = leader

    # Identifier
    f001 = xmlrecord["header"]["identifier"]
    regexp = re.match("oai:mediarep.org:doc/(\d+)", f001)
    if regexp:
        f001 = regexp.group(1)
        marcrecord.add("001", data="finc-170-" + f001)
    else:
        print("Der Identifier konnte nicht zerlegt werden: " + f001)

    # Zugangsfacette
    f007 = formats[format]["e007"]
    marcrecord.add("007", data=f007)

    # Periodizität
    if xmlrecord["metadata"]["oai_dc:dc"].get("dc:date"):
        year = xmlrecord["metadata"]["oai_dc:dc"]["dc:date"]
    else:
        year = ""
    periodicity = formats[format]["008"]
    language = xmlrecord["metadata"]["oai_dc:dc"]["dc:language"]
    if language == "de" or language == "deu":
        language = "ger"
    f008 = marc_build_field_008(year, periodicity, language)
    marcrecord.add("008", data=f008)

    # ISBN
    if xmlrecord["metadata"]["oai_dc:dc"].get("dc:identifier"):
        identifiers = xmlrecord["metadata"]["oai_dc:dc"]["dc:identifier"]
        if isinstance(identifiers, list):
            for identifier in identifiers:
                f020a = check_isbn(identifier)
                marcrecord.add("020", a=f020a)
                break

    # ISSN
    if xmlrecord["metadata"]["oai_dc:dc"].get("dc:identifier"):
        identifiers = xmlrecord["metadata"]["oai_dc:dc"]["dc:identifier"]
        if isinstance(identifiers, list):
            for identifier in identifiers:
                identifier = identifier.replace("issn:", "")
                f022a = check_issn(identifier)
                marcrecord.add("022", a=f022a)
                break

    # Sprache
    language = xmlrecord["metadata"]["oai_dc:dc"]["dc:language"]
    if language == "de" or language == "deu":
        language = "ger"
    marcrecord.add("041", a=language)

    # Fachgebiet
    if xmlrecord["metadata"]["oai_dc:dc"].get("dc:subject"):
        subjects = xmlrecord["metadata"]["oai_dc:dc"]["dc:subject"]
        if isinstance(subjects, list):
            for subject in subjects:
                if subject:
                    if re.search("\d\d\d", subject):
                        marcrecord.add("082", a=subject, _2="ddc")
                        break
        else:
            if re.search("\d\d\d", subjects):
                marcrecord.add("084", a=subjects, _2="ddc")

    # 1. Urheber
    if xmlrecord["metadata"]["oai_dc:dc"].get("dc:creator"):
        creator = xmlrecord["metadata"]["oai_dc:dc"]["dc:creator"]
        if isinstance(creator, list):
            marcrecord.add("100", a=creator[0])
        else:
            marcrecord.add("100", a=creator)

    # Titel
    f245 = xmlrecord["metadata"]["oai_dc:dc"]["dc:title"]
    marcrecord.add("245", a=f245)

    # Erscheinungsvermerk
    if xmlrecord["metadata"]["oai_dc:dc"].get("dc:publisher"):
        f260b = xmlrecord["metadata"]["oai_dc:dc"]["dc:publisher"]
    else:
        f260b = ""

    if xmlrecord["metadata"]["oai_dc:dc"].get("dc:date"):
        f260c = xmlrecord["metadata"]["oai_dc:dc"]["dc:date"]
    else:
        f260c = ""

    publisher = ["b", delistify(f260b), "c", delistify(f260c)]
    marcrecord.add("260", subfields=publisher)

    # RDA-Inhaltstyp
    f336b = formats[format]["336b"]
    marcrecord.add("336", b=f336b)

    # RDA-Datenträgertyp
    f338b = formats[format]["338b"]
    marcrecord.add("338", b=f338b)

    # Beschreibung
    if xmlrecord["metadata"]["oai_dc:dc"].get("dc:description"):
        f520a = xmlrecord["metadata"]["oai_dc:dc"]["dc:description"]
        marcrecord.add("520", a=f520a)

    # Schlagwörter
    if xmlrecord["metadata"]["oai_dc:dc"].get("dc:subject"):
        subjects = xmlrecord["metadata"]["oai_dc:dc"]["dc:subject"]
        if isinstance(subjects, list):
            for subject in subjects:
                if subject:
                    if not re.search("\d\d\d", subject):
                        marcrecord.add("650", a=subject)
        else:
            if not re.search("\d\d\d", subject):
                marcrecord.add("650", a=subject)

    # GND-Inhalts- und Datenträgertyp
    f655a = formats[format]["655a"]
    f6552 = formats[format]["6552"]
    marcrecord.add("655", a=f655a, _2=f6552)

    # weitere Urheber
    if xmlrecord["metadata"]["oai_dc:dc"].get("dc:creator"):
        creators = xmlrecord["metadata"]["oai_dc:dc"]["dc:creator"]
        if isinstance(creators, list):
            for creator in creators[1:]:
                marcrecord.add("700", a=creator)

    # Zeitschrift
    if xmlrecord["metadata"]["oai_dc:dc"].get("dc:source"):
        sources = xmlrecord["metadata"]["oai_dc:dc"]["dc:source"]
        if isinstance(sources, list):
            for source in sources:
                if ":" in source:
                    f773a = source.split("In: ")
                    if len(f773a) == 2:
                        marcrecord.add("773", a=f773a[1])
                    else:
                        marcrecord.add("773", a=f773a[0])
        else:
            f773a = sources.split("In: ")
            if len(f773a) == 2:
                marcrecord.add("773", a=f773a[1])
            else:
                marcrecord.add("773", a=f773a[0])

    # Link zu Datensatz und Ressource
    urls = xmlrecord["metadata"]["oai_dc:dc"]["dc:identifier"]
    for f856u in urls:
        if "https://mediarep.org" in f856u:
            marcrecord.add("856", q="text/html", _3="Link zur Ressource", u=f856u)
        elif "doi.org" in f856u:
            marcrecord.add("856", q="text/html", _3="Zitierlink (DOI)", u=f856u)

    # SWB-Inhaltstyp
    f935c = formats[format]["935c"]
    marcrecord.add("935", c=f935c)

    # Kollektion
    collection = ["a", f001, "b", "170", "c", "sid-170-col-mediarep"]
    marcrecord.add("980", subfields=collection)

    outputfile.write(marcrecord.as_marc())

inputfile.close()
outputfile.close()
