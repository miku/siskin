#!/usr/bin/env python3
# coding: utf-8

# SID: 150
# Ticket: #11832, #15175
# TechnicalCollectionID: sid-150-col-monami
# Task: HSMW

import io
import re
import sys

import xmltodict

import marcx


def ddcmatch(value):
    """
    Returns true, if given value matches profile.
    """
    if value in ("091", "092", "093", "095"):
        return True

    for prefix in ("002", "020", "021", "022", "023", "025", "026", "027", "028", "070", "090", "094", "760"):
        if value.startswith(prefix):
            return True
    return False


inputfilename = "150_input.xml"
outputfilename = "150_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:3]

inputfile = open(inputfilename, "rb")
outputfile = open(outputfilename, "wb")

xmlfile = inputfile.read()
xmlrecords = xmltodict.parse(xmlfile, force_list="dc:subject")

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
            ddc1 = setspec.replace("ddc:", "")
            break
    else:
        ddc1 = ""

    try:
        subjects = xmlrecord["metadata"]["oai_dc:dc"]["dc:subject"]
    except:
        subjects = []
    for subject in subjects:
        if "ddc" in subject:
            ddc2 = subject.replace("ddc:", "")
            break
    else:
        ddc2 = ""

    if len(ddc1) > len(ddc2):
        f082a = ddc1
    else:
        f082a = ddc2
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
        f689a = []
    for subject in f689a:
        if "ddc" not in subject:
            if " , " not in subject:
                marcrecord.add("689", a=subject)
            else:
                subjects = subject.split(" , ")
                for subject in subjects:
                    marcrecord.add("689", a=subject)

    # Link zu Datensatz und Ressource
    urls = xmlrecord["metadata"]["oai_dc:dc"]["dc:identifier"]
    for url in urls:
        if "docId/" in url:
            marcrecord.add("856", q="text/html", _3="Link zum Datensatz", u=url)
        elif ".pdf" in url or ".PDF" in url:
            marcrecord.add("856", q="text/html", _3="Link zur Ressource", u=url)

    # Medientyp
    marcrecord.add("935", b="cofz")
    marcrecord.add("935", c="hs")

    # Profilierung
    ddc = ddcmatch(f082a)
    if ddc:
        marcrecord.add("650", a="mitddc")
        f980c = ["sid-150-col-monami", "sid-150-col-monamibuch"]
    else:
        f980c = ["sid-150-col-monami"]
    marcrecord.add("980", a=f001, b="150", c=f980c)

    outputfile.write(marcrecord.as_marc())

inputfile.close()
outputfile.close()
