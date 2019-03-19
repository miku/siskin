#!/usr/bin/env python
# coding: utf-8


from __future__ import print_function

from builtins import *

import io
import sys
import re

import marcx
import xmltodict


lang_map = {"deutsch": "ger",
            "englisch": "eng",          
            "franzoesisch": "fre",
            "spanisch": "spa",
            "portugiesisch": "por",
            "italienisch": "ita",
            "tuerkisch": "tur",
            "schwedisch": "swe"}


# Default input and output.
inputfilename = "78_input.xml"
outputfilename = "78_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = io.open(inputfilename, "r", encoding='utf-8')
outputfile = io.open(outputfilename, "wb")
xmlfile = inputfile.read()
xmlrecords = xmltodict.parse(xmlfile)

for xmlrecord in xmlrecords["IZI_Datensaetze"]["Datensatz"]:
    
    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False

    # Leader
    marcrecord.leader = "     nab  22        4500"

    # Identifier
    f001 = xmlrecord["NUMMER"]
    if not f001:
        continue
    marcrecord.add("001", data="finc-78-" + f001)

    # 007
    marcrecord.add("007", data="cr")

    # ISBN
    isbns = xmlrecord["ISBN"]
    if isbns:
        isbns = isbns.split("; ")
        for f020a in isbns:
            marcrecord.add("020", a=f020a)
    
    # Sprache   
    languages = xmlrecord["SPRACHE"]
    if languages:
        languages = languages.split("; ")
        language = languages[0]
        language = lang_map.get(language, "")
        if language != "":
            marcrecord.add("008", data="130227uu20uuuuuuxx uuup%s  c" % language)
            if len(languages) < 4:
                subfields = []
                for f041a in languages:
                    subfields.append("a")
                    f041a = lang_map.get(f041a, "")
                    subfields.append(f041a)
                    marcrecord.add("041", subfields=subfields)
            else:
                marcrecord.add("041", a="mul")
        else:
            print("Die Sprache %s fehlt in der Lang_Map!" % language)
   
    # 1. Urheber
    persons = xmlrecord["AUTOR"]
    if persons:
        persons = persons.split("; ")
        f100a = persons[0]
        marcrecord.add("100", a=f100a)
    else:
        persons = []

    # körperschaftlicher Urheber
    corporates = xmlrecord["KORP_URHEBER"]
    if corporates:
        corporates = corporates.split("; ")
        f110a = corporates[0]
        marcrecord.add("110", a=f110a)

    # Haupttitel
    f245a = xmlrecord["TITEL"]
    f245b = xmlrecord["BAND"]
    if f245b:
        marcrecord.add("245", a=f245a, b=f245b)
    else:
        marcrecord.add("245", a=f245a)

    # Erscheinungsvermerk
    f260c = xmlrecord["JAHR"]
    marcrecord.add("260", c=f260c)

    # Umfangsangabe
    f300a = xmlrecord["QUELLE"]
    if f300a:
        match = re.search("(.*?):\s(.*?)\s(\d\d\d\d),\s(.*)", f300a) # Quelle matchwn (Seiten vielleicht 773w?)
        if match:
            f300a, f300b = match.groups()
            marcrecord.add("300", a=f300a)

    # Reihentitel
    f490a = xmlrecord["REIHENTITEL"]
    if f490a:
        f490a = f490a.replace("(", "")
        f490a = f490a.replace(")", "")
        f490a = re.sub("\.\s(\d)", r" ; \1", f490a)
        marcrecord.add("490", a=f490a)

    # Fußnote
    languages = xmlrecord["SPRACHE"]
    if languages:
        numlang = languages.split()
        if len(numlang) > 3:
            languages = languages.replace(";", ",")
            f500a = "Text auf " + languages.title()
            marcrecord.add("500", a=f500a)

    # Schlagwort
    subjects = xmlrecord["SCHLAGWORT"]
    subjects = subjects.split("; ")
    for f650a in subjects:
        marcrecord.add("650", a=f650a)

    # weitere Urheber
    for f700a in persons[1:]:
        marcrecord.add("700", a=f700a)

    # weitere körperschaftliche Urheber
    if corporates:
        for f710a in corporates[1:]:
            if f710a:
                f710a = f710a.replace(" (Hrsg.)", "")       
                marcrecord.add("710", a=f710a)

    # körperschaftliche Herausgeber
    corporate_editors = xmlrecord["KORP_HRSG"]
    if corporate_editors:
        corporate_editors = corporate_editors.split("; ")
        for f710a in corporate_editors[1:]:
            if f710a:
                f710a = f710a.replace(" (Hrsg.)", "")
                marcrecord.add("710", a=f710a)

    # übergeordnete Ressource
    f773w = xmlrecord["ZEITSCHRIFT"]
    marcrecord.add("773", w=f773w)

    f773t = xmlrecord["SAMMELWERK"]
    marcrecord.add("773", t=f773t)

    # Link zur Ressource
    f856u = xmlrecord["URL"]
    if f856u != "":
        marcrecord.add("856", q="text/html", _3="Link zur Ressource", u=f856u)
    
    # Medienform
    marcrecord.add("935", b="cofz")

    # Kollektion
    marcrecord.add("980", a=f001, b="78", c="sid-78-col-izi")

    outputfile.write(marcrecord.as_marc())
   

inputfile.close()
outputfile.close()
