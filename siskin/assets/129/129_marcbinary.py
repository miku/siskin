#!/usr/bin/env python3
# coding: utf-8


# SID: 129
# Collection: geoscan
# refs: #9871


import io
import re
import sys
import json
import marcx
import pymarc
import feedparser


langmap = {
    "English": "eng",
    "French": "fre",
    "Russian": "rus",
    "Greek": "gre"
}


def get_field(tag):  
    try:
        return record[tag]
    except:
        return ""


inputfilename, outputfilename = "129_input.xml", "129_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

outputfile = io.open(outputfilename, "wb")

records = feedparser.parse(inputfilename)

for record in records.entries:

    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False

    # Leader
    marcrecord.leader = "     naa  22        4500"

    # Identifikator
    f001 = get_field("guid")
    marcrecord.add("001", data="finc-126-" + f001)
    
    # Format
    marcrecord.add("007", data="cr")

    # Sprache
    languages = get_field("geoscan_language")
    if ";" in languages:
        languages = languages.split("; ")
        lang = []
        for language in languages:
            f041a = langmap.get(language, "")
            if f041a != "":
                lang.append("a")
                lang.append(f041a)
                marcrecord.add("041", subfields=lang)                
            else:                
                print("Die folgende Sprache fehlt in der Langmap: %s" % language)
    else:
        f041a = langmap.get(languages, "")
        marcrecord.add("041", a=f041a)

    # 1. Urheber
    f100a = get_field("geoscan_author")
    if ";" in f100a:
        f100a = f100a.split("; ")
        f100a = f100a[0]
    marcrecord.add("100", a=f100a)

    # Haupttitel    
    f245a = get_field("title")    
    marcrecord.add("245", a=f245a)

    # Verlag
    f260a = get_field("geoscan_publisher")
    description = get_field("description")
    regexp = re.search("(\d\d\d\d),\s(.*?),\s(https.*)", description)
    if regexp:
        f260c, f300a, f500a = regexp.groups()
        f260c = ", " + f260c
    else:
        print("Der folgende String konnte nicht mittels regulärer Ausdrücke zerlegt werden: %s" % description)
        f260c = ""
        f300a = ""
        f500a = ""
    marcrecord.add("260", a=f260a, c=f260c)

    # Seitenzahl
    marcrecord.add("300", a=f300a)

    # Anzahl der Karten
    f300a = get_field("geoscan_maps")
    marcrecord.add("300", a=f300a)

    # DOI
    marcrecord.add("500", a=f500a)

    # Angaben zur Karte
    f500a = get_field("geoscan_mapinfo")
    marcrecord.add("500", a=f500a)

    # Kurzreferat
    f520a = get_field("geoscan_abstract")
    marcrecord.add("520", a=f520a)

    # Schlagwörter
    subject = get_field("geoscan_province")
    if ";" in subject:
        subjects = subject.split("; ")
        for subject in subjects:
            marcrecord.add("650", a=subject)
    else:
        marcrecord.add("650", a=subject)
    
    subject = get_field("geoscan_area")
    if ";" in subject:
        subjects = subject.split("; ")
        for subject in subjects:
            marcrecord.add("650", a=subject)
    else:
        marcrecord.add("650", a=subject)

    # weitere Urheber
    f700a = get_field("geoscan_author")
    if ";" in f700a:
        authors = f700a.split("; ")
        for f700a in authors[1:]:
            marcrecord.add("700", a=f700a)

    # URL
    f856u = get_field("link")
    marcrecord.add("856", q="text/html", _3="Link zur Ressource", u=f856u)
    
    # 980
    collections = ["a", f001, "b", "129", "c", "geoscan"]
    marcrecord.add("980", subfields=collections)
   
    outputfile.write(marcrecord.as_marc())

outputfile.close()