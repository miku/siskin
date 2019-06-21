#!/usr/bin/env python3
# coding: utf-8

# SID: 20
# Ticket: #14793
# TechnicalCollectionID: sid-142-col-???
# Task: TODO


from __future__ import print_function

import re
import sys

import xmltodict

import marcx
from siskin.mappings import formats
from siskin.utils import xmlstream, marc_build_field_008, check_isbn, check_issn

setlist = ["gallica:typedoc:partitions"]

inputfilename = "20_input.xml"
outputfilename = "20_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

outputfile = open(outputfilename, "wb")
i = 1
for oldrecord in xmlstream(inputfilename, "Record"):

    if i == 1000:
        #break
        pass
    
    oldrecord = xmltodict.parse(oldrecord, force_list=("dc:identifier", "dc:creator", "dc:title", "dc:publisher", "dc:rights", "dc:subject"))
    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False

    setspec = oldrecord["Record"]["header"]["setSpec"]

    if setspec in setlist:
        i += 1
    else:
        continue

    oldrecord = oldrecord["Record"]["metadata"]["ns0:dc"]

    title = oldrecord.get("dc:title", "")
    if not title:
        continue

    # Formatmapping
    if setspec == "gallica:typedoc:partitions":
        format = "Score"

    # Leader
    leader = formats[format]["Leader"]
    marcrecord.leader = leader

    # Identifikator
    id = oldrecord["dc:identifier"][0]
    match = re.search("gallica.bnf.fr/ark:/\d+/(.*)", id)
    if match:
        f001 = match.group(1)
        f980a = match.group(1)
    else:
        sys.exit("Die ID konnte nicht ermittelt werden: " + f001)
    f001 = "finc-20-" + f001
    marcrecord.add("001", data=f001)

    # Zugangsart
    if setspec == "gallica:typedoc:partitions":
        f007 = formats[format]["e007"]
    else:
        f007 = formats[format]["p007"]
    marcrecord.add("007", data=f007)

    # Periodizität
    year = oldrecord.get("dc:date", "")
    periodicity = formats[format]["008"]
    language = oldrecord.get("dc:language", "")
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
    f041a = oldrecord.get("dc:language", "ger")
    if f041a and len(f041a) != 3:
        sys.exit("Die Sprache ist nicht MARC-konform: " + f041a)
    marcrecord.add("041", a=f041a)

    # 1. Schöpfer
    creators = oldrecord.get("dc:creator", "")
    if creators:
        match = re.search("(.*?)\s\(([0-9\.-]{9,9})\)\.\s(.*)", creators[0])
        if match:
            f100a, f100d, f100e = match.groups()
        else:
            f100a = creator
            f100d = ""
            f100e = ""
        marcrecord.add("100", a=f100a, d=f100d, e=f100e)
   
    # Haupttitel
    titles = oldrecord["dc:title"]
    if len(titles) == 1:
        f245 = titles[0]
    else:
        f245 = titles[0]["#text"]
    f245 = f245.split(" : ")
    if len(f245) == 2:
        f245a = f245[0]
        f245b = f245[1]
    else:
        f245a = f245[0]
        f245b = ""
    marcrecord.add("245", a=f245a, b=f245b)
  
    # Alternativtitel
    if len(titles) == 2:
        f246a = titles[0]["#text"]
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
    f300a = oldrecord["dc:format"]
    marcrecord.add("300", a=f300a)

    # RDA-Inhaltstyp
    f336b = formats[format]["336b"]
    marcrecord.add("336", b=f336b)

    # RDA-Datenträgertyp
    f338b = formats[format]["338b"]
    marcrecord.add("338", b=f338b)

    # Rechtshinweis
    f500a = oldrecord["dc:rights"]
    f500a = f500a[1]["#text"].title()
    marcrecord.add("500", a=f500a)

    # Inhaltsbeschreibung
    f520a = oldrecord.get("dc:description", "")
    marcrecord.add("520", a=f520a)

    # Schlagwort
    subjects = oldrecord.get("dc:subject", "")
    for subject in subjects:
        f650a = subject["#text"]
        marcrecord.add("650", a=f650a)

    # GND-Inhalts- und Datenträgertyp
    f655a = formats[format]["655a"]
    f6552 = formats[format]["6552"]
    marcrecord.add("338", a=f655a, _2=f6552)

    # weitere geistige Schöpfer
    creators = oldrecord.get("dc:creator", "")
    for creator in creators[1:]:
        match = re.search("(.*?)\s\(([0-9\.-]{9,9})\)\.\s(.*)", creator)
        if match:
            f700a, f700d, f700e = match.groups()
        else:
            f100a = creator
            f100d = ""
            f100e = ""
        marcrecord.add("700", a=f700a, d=f700d, e=f700e)

    # Link zu Datensatz und Ressource
    f856u = oldrecord["dc:relation"]
    f8563 = "Link zum Datensatz"
    marcrecord.add("856", q="text/html", _3=f8563, u=f856u)

    # SWB-Inhaltstyp
    f935c = formats[format]["935c"]
    marcrecord.add("935", c=f935c)

    # Ansigelung
    collections = ["a", f980a, "b", "20", "c", "sid-142-col-gallica"]
    marcrecord.add("980", subfields=collections)

    outputfile.write(marcrecord.as_marc())

outputfile.close()