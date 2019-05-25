#!/usr/bin/env python3
# coding: utf-8

# Quelle: VDEH
# SID: 130
# Tickets: #9868, #15025
# technicalCollectionID: sid-130-col-vdeh
# Task: vdeh.py

from __future__ import print_function

import collections
import io
import re
import sys
from builtins import *

import requests
import xmltodict

import marcx
from siskin.mab import MabXMLFile
from siskin.utils import marc_build_imprint

formatmap = {
    "p||||||z|||||||": u"Zeitschrift",
    "z||||||z|||||||": u"Zeitung",
    "r||||||z|||||||": u"Reihe",
    "r||||||||||||||": u"Reihe",
    "a|a|||||||||||": u"Monografie",
    "a|||||||||||||": u"Monografie",
    "a|a|||||||": u"Monografie",
    "|||||ca||||||": u"Videokassette",
    "a|||||||g|||||": u"Datenträger",
    "||||||||g|": u"Datenträger"
}


def check_swb_isbn(isbn):
    """
    The length of the response can indicate, whether records exist.
    """
    req = requests.get("%s?q=source_id:0+institution:DE-105+isbn:%s&wt=csv&fl=id" % (servername, isbn))
    return len(req.text)


def check_swb_issn(issn, title):
    """
    The length of the response can indicate, whether records exist.
    """
    req = requests.get('%s?q=source_id:0+institution:DE-105+issn:%s+title_short:"%s"&wt=csv&fl=id' %
                       (servername, issn, title))
    return len(req.text)


inputfilename = "130_input.xml"
outputfilename = "130_output.mrc"
servername = "https://index.ub.uni-leipzig.de/solr/biblio/select"

if len(sys.argv) == 4:
    inputfilename, outputfilename, servername = sys.argv[1:]

reader = MabXMLFile(inputfilename, replace=((u"¬", ""), ))
outputfile = open(outputfilename, "wb")

hierarchymap = collections.defaultdict(list)

for record in reader:

    f010 = ""
    f089 = ""

    f010 = record.field("010")
    f089 = record.field("089")

    if f010 and f089:
        f010 = f010.lstrip("0")
        hierarchymap[f010].append(f089)

for record in reader:

    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False

    subjects = []
    persons = {}
    corporates = []

    # Formatmapping
    format1 = record.field("052", alt="")
    format2 = record.field("050", alt="")

    if format1 != "" or format2 != "":
        format = format1 or format2
    else:
        format = "a|a|||||||||||"

    if formatmap.get(format) == "Zeitschrift":
        leader = "     cas a2202221   4500"
        f007 = "tu"
        f008 = "                     p"
        f935b = "druck"
        f935c = "az"
    elif formatmap.get(format) == "Zeitung":
        leader = "     cas a2202221   4500"
        f007 = "tu"
        f008 = "                     n"
        f935b = "druck"
        f935c = "az"
    elif formatmap.get(format) == "Reihe":
        leader = "     cas a2202221   4500"
        f007 = "tu"
        f008 = "                     m"
        f935b = "druck"
        f935c = "az"
    elif formatmap.get(format) == "Monografie":
        leader = "     nam  22        4500"
        f007 = "tu"
        f008 = ""
        f935b = "druck"
        f935c = ""
    elif formatmap.get(format) == "Videokassette":
        leader = "     cgm  22        4500"
        f007 = "v"
        f008 = ""
        f935b = "vika"
        f935c = "vide"
    elif formatmap.get(format) == "Datenträger":
        leader = "     cgm  22        4500"
        f007 = "v"
        f008 = ""
        f935b = "soerd"
        f935c = ""
    else:
        leader = "     nam  22        4500"
        f007 = "tu"
        f008 = ""
        f935b = "druck"
        f935c = ""

    assert (len(leader) == 24)
    marcrecord.leader = leader

    # Identfikator
    f001 = record.field("001", alt="")
    if f001 == "":
        continue
    marcrecord.add("001", data="finc-130-" + f001)

    # Format
    marcrecord.add("007", data=f007)
    marcrecord.add("008", data=f008)

    # ISBN
    f020a = record.field("540", alt="")
    if f020a:
        match = re.search("([0-9xX-]{10,17})", f020a)
        if match:
            f020a = match.group(1)
            x = check_swb_isbn(f020a)
            if x > 3:
                continue
            marcrecord.add("020", a=f020a)

    # ISSN
    f022a = record.field("542", alt="")
    if f022a:
        match = re.search("([0-9xX-]{9,9})", f022a)
        if match:
            f022a = match.group(1)
            x = check_swb_issn(f022a, f245a)
            if x > 3:
                continue
            marcrecord.add("022", a=f022a)

    # Sprache
    languages = record.field("037", alt="")
    f041a = [languages[i:i + 3]
             for i in range(0, len(languages), 3)]  # to handle uncommon language codes like "gerengfre"
    marcrecord.add("041", a=f041a)

    # 1. Urheber
    f100a = record.field("100", alt="")
    f100e = ""
    if f100a:
        match = re.search(u".\s(\[.*\])", f100a)
        if match:
            f100e = match.group(1)
            f100a = re.sub(u".\s\[.*\]", "", f100a)
        marcrecord.add("100", a=f100a, e=f100e)

    # 1. Körperschaft
    f110a = record.field("200", alt="")
    marcrecord.add("110", a=f110a)

    # Titel und Verantwortlichenangabe
    f245a = record.field("331", alt="")
    f245b = record.field("335", alt="")
    f245c = record.field("359", alt="")
    if len(f245a) < 3 or "Arkady" in f245a:
        # einzelne Zeitschriftenhefte, Titel nur aus Komma und Leerzeichen und die fehlerhaften Arkady-Records werden übersprungen
        continue
    titleparts = ["a", f245a, "b", f245b, "c", f245c]
    marcrecord.add("245", subfields=titleparts)

    # Ausgabe
    f250a = record.field("403", alt="")
    marcrecord.add("250", a=f250a)

    # Erscheinungsvermerk
    f260a = record.field("410", alt="")
    if not f260a:
        f260a = record.field("419", "a", alt="")

    f260b = record.field("412", alt="")
    if not f260b:
        f260b = record.field("419", "b", alt="")

    f260c = record.field("425", alt="")
    if not f260c:
        f260c = record.field("419", "c", alt="")

    subfields = marc_build_imprint(f260a, f260b, f260c)
    marcrecord.add("260", subfields=subfields)

    # Umfang
    f300a = record.field("433", alt="")
    f300b = record.field("434", alt="")
    if f300a and f300b:
        f300b = " : " + f300b
    f300c = record.field("435")
    if (f300a or f300b) and f300c:
        f300c = " ; " + f300c
    physicaldescription = ["a", f300a, "b", f300b, "c", f300c]
    marcrecord.add("300", subfields=physicaldescription)

    # Schlagwörter
    subjects = record.fields("710")
    for f650a in subjects:
        marcrecord.add("650", a=f650a)

    # weitere Personen
    for i in range(101, 197):
        f700a = record.field(str(i), alt="")
        match = re.search("^\d+", f700a)  # die Felder, die nur Personen-IDs enthalten, werden übersprungen
        if not match:
            match = re.search(".\s(\[.*\])", f700a)
            if match:
                f700e = match.group(1)
                f700a = re.sub(".\s\[.*\]", "", f700a)
            else:
                f700e = ""
            persons[f700a] = f700e

    for f700a, f700e in persons.items():
        marcrecord.add("700", a=f700a, e=f700e)

    # weitere Körperschaften
    for i in range(201, 299):
        f710a = record.field(i, alt="")
        match = re.search("^\d+", f710a)  # die Felder, die nur Körperschafts-IDs enthalten, werden übersprungen
        if not match:
            corporates.append(f710a)

    for f710a in corporates:
        marcrecord.add("710", a=f710a)

    # Link zum Datensatz
    f856u = record.field("655", "u", alt="")
    if f856u:
        marcrecord.add("856", q="text/html", u=f856u)

    #Bestandsnachweis (866a)
    f866a = f001.lstrip("0")
    f866a = hierarchymap.get(f866a, "")
    f866a = "; ".join(f866a)
    if not isinstance(f866a, str):
        f866a = f866a.decode('utf-8')
    marcrecord.add("866", a=f866a)

    # SWB-Format
    marcrecord.add("935", b=f935b, c=f935c)

    # Kollektion
    marcrecord.add("980", a=f001, b="130", c="sid-130-col-vdeh")

    outputfile.write(marcrecord.as_marc())

outputfile.close()
