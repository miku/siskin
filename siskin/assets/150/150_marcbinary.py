#!/usr/bin/env python3
# coding: utf-8

import io
import re
import sys

import xmltodict

import marcx

inputfilename = "150_input.xml"
outputfilename = "150_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:3]

inputfile = open(inputfilename, "rb")
outputfile = open(outputfilename, "wb")

xmlfile = inputfile.read()
xmlrecords = xmltodict.parse(xmlfile)

for xmlrecord in xmlrecords["Records"]["Record"]:

    try:
        f245 = xmlrecord["metadata"]["oai_dc:dc"]["dc:title"]
    except:
        continue

    # Closed Access werden übersprungen
    access = xmlrecord["metadata"]["oai_dc:dc"]["dc:rights"][1]
    if access == "info:eu-repo/semantics/closedAccess":
        continue

    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False

    # Leader
    marcrecord.leader = "     cam  22        4500"

    # Identifier
    f001 = xmlrecord["header"]["identifier"]
    regexp = re.match("oai:opus.bsz-bw.de-hsmw:(\d+)", f001)
    regexp2 = re.match("oai:monami.hs-mittweida.de:(\d+)", f001)
    if regexp:
        f001 = regexp.group(1)
    elif regexp2:
        f001 = regexp2.group(1)
    else:
        sys.exit("Keine ID vorhanden!" + f001)
    marcrecord.add("001", data="finc-150-" + f001)

    # 007
    marcrecord.add("007", data="cr")

    # Sprache
    language = xmlrecord["metadata"]["oai_dc:dc"]["dc:language"]
    if language == "deu":
        language = "ger"
    marcrecord.add("008", data="130227uu20uuuuuuxx uuup%s  c" % language)
    
    # DDC
    setspecs = xmlrecord["header"]["setSpec"]
    for setspec in setspecs:
        if "ddc:" in setspec:
            f082a = setspec.replace("ddc:", "")
            marcrecord.add("082", a=f082a)

    # Verfasser
    try:
        f100a = xmlrecord["metadata"]["oai_dc:dc"]["dc:creator"]
    except:
        f100a = ""
    marcrecord.add("100", a=f100a)

    # Titel
    f245 = xmlrecord["metadata"]["oai_dc:dc"]["dc:title"]
    if isinstance(f245, list):
        f245a = f245[0]["#text"]
        f245b = f245[1]["#text"]
        marcrecord.add("245", a=f245a, b=f245b)
    else:
        marcrecord.add("245", a=f245["#text"])

    # Erscheinungsvermerk
    f260c = xmlrecord["metadata"]["oai_dc:dc"]["dc:date"]
    publisher = ["b", "Hochschule Mittweida,", "c", f260c]
    marcrecord.add("260", subfields=publisher)

    # Abstract
    try:
        f520a = xmlrecord["metadata"]["oai_dc:dc"]["dc:description"]["#text"]
        marcrecord.add("520", a=f520a)
    except:
        pass

    # Schlagwörter
    try:
        f689a = xmlrecord["metadata"]["oai_dc:dc"]["dc:subject"]
    except:
        f689a = ""
    if isinstance(f689a, list):
        for subject in f689a:
            if "ddc" not in subject:
                marcrecord.add("689", a=subject)
    else:
        f689a = f689a.split(" , ")
        if len(f689a) > 1:
            for subject in f689a:
                marcrecord.add("689", a=subject)

    # Link zu Datensatz und Ressource
    # Eventuell überprüfen, ob Reihenfolge der Links stimmt "files"
    f856u = xmlrecord["metadata"]["oai_dc:dc"]["dc:identifier"]
    filecheck = str(f856u)
    if ".pdf" not in filecheck and ".PDF" not in filecheck:
        continue
    marcrecord.add("856", q="text/html", _3="Link zum Datensatz", u=f856u[0])
    if len(f856u) == 2:
        marcrecord.add("856", q="text/pdf", _3="Link zur Ressource", u=f856u[1])
    else:
        marcrecord.add("856", q="text/pdf", _3="Link zur Ressource", u=f856u[2])

    # Medientyp
    marcrecord.add("935", b="cofz")
    marcrecord.add("935", c="hs")

    # Profilierung
    marcrecord.add("980", a=f001, b="150", c="sid-150-col-monami")

    outputfile.write(marcrecord.as_marc())

inputfile.close()
outputfile.close()
