#!/usr/bin/env python3
# coding: utf-8

# SID: 20
# Ticket: #14793
# TechnicalCollectionID: sid-20-col-gallica, sid-20-col-gallicabuch
# Task: gallica

from __future__ import print_function

import re
import sys

import xmltodict

import marcx
from siskin.mappings import formats, roles
from siskin.utils import (check_isbn, check_issn, marc_build_field_008, xmlstream)

setlist = ["gallica:typedoc:partitions", "gallica:theme:0:00", "gallica:theme:0:02"]

inputfilename = "20_input.xml"
outputfilename = "20_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

outputfile = open(outputfilename, "wb")

for oldrecord in xmlstream(inputfilename, "Record"):

    oldrecord = xmltodict.parse(oldrecord,
                                force_list=("setSpec", "dc:identifier", "dc:language", "dc:creator", "dc:title",
                                            "dc:publisher", "dc:rights", "dc:subject", "dc:relation", "dc:description"))
    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False

    status = oldrecord["Record"]["header"]["@status"]
    setspec = oldrecord["Record"]["header"]["setSpec"][0]
    metadata = oldrecord["Record"]["metadata"]

    if setspec not in setlist or not metadata or status == "deleted":
        continue

    oldrecord = oldrecord["Record"]["metadata"]["ns0:dc"]

    # Formatmapping
    if setspec == "gallica:typedoc:partitions":
        format = "Score"
    else:
        format = "Book"

    # Leader
    leader = formats[format]["Leader"]
    marcrecord.leader = leader

    # Identifikator
    id = oldrecord["dc:identifier"][0]
    match = re.search("gallica.bnf.fr/ark:/\d+/(.*)", id)
    if match:
        f001 = match.group(1)
        f001 = f001.rstrip("/date")
        f980a = match.group(1)
        f980a = f980a.rstrip("/date")

    else:
        sys.exit("Die ID konnte nicht ermittelt werden: " + f001)
    f001 = "finc-20-" + f001
    marcrecord.add("001", data=f001)

    # Zugangsart
    f007 = formats[format]["e007"]
    marcrecord.add("007", data=f007)

    # Periodizität
    year = oldrecord.get("dc:date", "")
    periodicity = formats[format]["008"]
    language = oldrecord.get("dc:language", [""])
    language = language[0]
    f008 = marc_build_field_008(year, periodicity, language)
    marcrecord.add("008", data=f008)

    # ISBN und ISSN
    numbers = oldrecord["dc:identifier"]
    for number in numbers:
        if "ISBN" in number:
            f020a = check_isbn(number)
            marcrecord.add("020", a=f020a)
        if "ISSN" in number:
            f022a = check_issn(number)
            marcrecord.add("022", a=f022a)

    # Sprache
    f041a = oldrecord.get("dc:language", [""])
    f041a = f041a[0]
    if f041a and len(f041a) != 3:
        sys.exit("Die Sprache ist nicht MARC-konform: " + f041a)
    marcrecord.add("041", a=f041a)

    # DDC-Notation
    if setspec == "gallica:theme:0:00":
        marcrecord.add("082", a="000")
    elif setspec == "gallica:theme:0:02":
        marcrecord.add("082", a="020")
    elif setspec == "gallica:typedoc:partitions":
        marcrecord.add("082", a="780")

    # 1. Schöpfer
    creators = oldrecord.get("dc:creator", "")
    if creators:
        match = re.search("(.*?)\s\(([0-9\.-]{9,9})\)\.\s(.*)", creators[0])
        if match:
            f100a, f100d, f100e = match.groups()
            f100e = f100e.lower()
            f1004 = roles.get(f100e, "")
            if not f1004:
                #print("Die Rollenbezeichnung '%s' fehlt in der Mapping-Tabelle." % f100e)
                pass
        else:
            f100a = creators[0]
            f100d = ""
            f1004 = ""
        marcrecord.add("100", a=f100a, d=f100d, _4=f1004)

    # Haupttitel, Titelzusatz und Verantwortliche
    titles = oldrecord["dc:title"]
    if len(titles) == 1:
        f245 = titles[0]
    else:
        if "#text" in titles[0] and "@xml:lang" in titles[0]:
            # [OrderedDict([('@xml:lang', '')]), OrderedDict([('@xml:lang', ''), ('#text', 'Andante')])]
            f245 = titles[0]["#text"]
        elif "@xml:lang" in titles[0] and not "#text" in titles[0]:
            f245 = titles[1]["#text"]
        else:
            f245 = titles[0]
    match1 = re.search("(.*?)\s:\s(.*)\s\/\s(.*)", f245)
    match2 = re.search("(.*)\s:\s(.*)", f245)
    match3 = re.search("(.*)\s\/\s(.*)", f245)
    if match1:
        f245a = match1.group(1)
        f245b = match1.group(2)
        f245c = match1.group(3)
    elif match2:
        f245a = match2.group(1)
        f245b = match2.group(2)
        f245c = ""
    elif match3:
        f245a = match3.group(1)
        f245b = ""
        f245c = match3.group(2)
    else:
        f245a = f245
        f245b = ""
        f245c = ""
    subfields = ["a", f245a, "b", f245b, "c", f245c]
    marcrecord.add("245", subfields=subfields)

    # Alternativtitel
    if len(titles) == 2:
        if "@xml:lang" in titles[1]:
            f246a = titles[1]["#text"]
        else:
            f246a = titles[1]
        if f246a != f245:
            marcrecord.add("246", a=f246a)

    # Erscheinungsvermerk
    f260a = ""
    f260b = ""
    publishers = oldrecord.get("dc:publisher", "")
    for publisher in publishers:
        match = re.search("(.*)\s\((.*)\)", publisher)
        if match:
            f260a, f260b = match.groups()
        else:
            f260a = publisher
            f260b = ""
        f260c = oldrecord.get("dc:date", "")
        subfields = ["a", f260a, "b", f260b, "c", f260c]
        marcrecord.add("260", subfields=subfields)

    # Umfangsangabe
    f300a = oldrecord.get("dc:format", "")
    marcrecord.add("300", a=f300a)

    # RDA-Inhaltstyp
    f336b = formats[format]["336b"]
    marcrecord.add("336", b=f336b)

    # RDA-Datenträgertyp
    f338b = formats[format]["338b"]
    marcrecord.add("338", b=f338b)

    # Rechtehinweis
    f500a = oldrecord.get("dc:rights", [""])
    if len(f500a) == 2:
        f500a = f500a[1]["#text"].title()
    else:
        f500a = f500a[0].title()
    marcrecord.add("500", a=f500a)

    # Inhaltsbeschreibung
    descriptions = oldrecord.get("dc:description", [""])
    for f520a in descriptions:
        if f520a:  # sometimes None
            if len(f520a) > 8000:
                f520a = f520a[:8000]
            marcrecord.add("520", a=f520a)

    # Schlagwort
    subjects = oldrecord.get("dc:subject", "")
    for subject in subjects:
        f650a = subject["#text"]
        marcrecord.add("650", a=f650a)

    # GND-Inhalts- und Datenträgertyp
    f655a = formats[format]["655a"]
    f6552 = formats[format]["6552"]
    marcrecord.add("655", a=f655a, _2=f6552)

    # weitere geistige Schöpfer
    creators = oldrecord.get("dc:creator", "")
    for creator in creators[1:]:
        match = re.search("(.*?)\s\(([0-9\.-]{9,9})\)\.\s(.*)", creator)
        if match:
            f700a, f700d, f700e = match.groups()
            f700e = f700e.lower()
            f7004 = roles.get(f700e, "")
            if not f7004:
                #print("Die Rollenbezeichnung '%s' fehlt in der Mapping-Tabelle." % f700e)
                pass
        else:
            f700a = creator
            f700d = ""
            f7004 = ""
        marcrecord.add("700", a=f700a, d=f700d, _4=f7004)

    # Link zu Datensatz und Ressource
    f856u = oldrecord["dc:relation"][0]

    match = re.search("(http.*catalogue.*)", f856u)
    if match:
        # Notice du catalogue : http://catalogue.bnf.fr/ark:/12148/cb42874085r
        f856u = match.group(1)
        marcrecord.add("856", q="text/html", _3="Link zum Datensatz", u=f856u)

    f856u = oldrecord["dc:identifier"][0]
    marcrecord.add("856", q="image/jpeg", _3="Link zum Digitalisat", u=f856u)

    # SWB-Inhaltstyp
    f935c = formats[format]["935c"]
    marcrecord.add("935", c=f935c)

    # Ansigelung
    if setspec == "gallica:theme:0:00":
        f980c = "sid-20-col-gallicabuch"
    elif setspec == "gallica:theme:0:02":
        f980c = "sid-20-col-gallicabuch"
    else:
        f980c = "sid-20-col-gallica"
    collections = ["a", f980a, "b", "20", "c", f980c]
    marcrecord.add("980", subfields=collections)

    outputfile.write(marcrecord.as_marc())

outputfile.close()
