#!/usr/bin/env python
# coding: utf-8

from builtins import *

import io
import sys
import re
import base64

import marcx
import xmltodict

lang_map = {
    "deu": "ger"
}

# Default input and output.
inputfilename = "73_input.xml" 
outputfilename = "73_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = io.open(inputfilename, "rb")
outputfile = io.open(outputfilename, "wb")
xmlfile = inputfile.read()
xmlrecords = xmltodict.parse(xmlfile)

for xmlrecord in xmlrecords["Records"]["Record"]:
    
    marcrecord = marcx.Record(force_utf8=True)

    # Leader
    marcrecord.leader = "     nab  22        4500"

    # Identifier
    f001 = xmlrecord["header"]["identifier"]
    f001 = f001.encode("utf-8")
    f001 = base64.b64encode(f001)
    f001 = f001.decode("ascii")
    marcrecord.add("001", data="finc-73-" + f001)

    # 007
    marcrecord.add("007", data="cr")

    # ISSN
    try:
        f022 = xmlrecord.get("metadata").get("oai_dc:dc").get("dc:source", "")
        f022a = f022[1]
        # elektronisch
        marcrecord.add("022", a=f022a)
        # print
        f022a = f022[2]
        marcrecord.add("022", a=f022a)
    except:
        pass

    # Sprache
    try:
        language = xmlrecord.get("metadata").get("oai_dc:dc").get("dc:language", "")
        f041a = lang_map.get(language, "")
        if f041a != "":
            marcrecord.add("008", data="130227uu20uuuuuuxx uuup%s  c" % f041a)
            marcrecord.add("041", a=f041a)          
        else:
            print("Die Sprache %s fehlt in der Lang_Map!" % language)
    except:
        pass

    # 1. Urheber
    try:
        f100a = xmlrecord.get("metadata").get("oai_dc:dc").get("dc:creator", "")
        marcrecord.add("100", a=f100a)
    except:
        pass

    # Haupttitel, Titelzusatz, Verantwortlichenangabe
    try:
        f245 = xmlrecord.get("metadata").get("oai_dc:dc").get("dc:title", "").get("#text")
        f245 = f245.split(":")
        f245a = f245[0] 
        f245a = f245a.strip(" ")
        if len(f245) > 1:
            f245a += " – " + f245[1].strip(" ")
        if len(f245) > 2: 
            f245b = ""       
            for titlepart in f245[2:]:
                titlepart = titlepart.strip(" ")
                f245b += titlepart + " : "
        else:
            f245b = ""        
        marcrecord.add("245", a=f245a, b=f245b.rstrip(" : "))
    except:
        continue

    # Erscheinungsvermerk
    f260c = xmlrecord.get("metadata").get("oai_dc:dc").get("dc:date", "")
    regexp = re.search("(\d\d\d\d)", f260c)
    if regexp:
        f260c = regexp.group(1)
    publisher = ["a", "Marburg", "b", " : " + "Schüren Verlag, ", "c", f260c]
    marcrecord.add("260", subfields=publisher)

    # Schlagwort
    try:
        f689a = xmlrecord.get("metadata").get("oai_dc:dc").get("dc:subject", "").get("#text")
        marcrecord.add("689", a=f689a)
    except:
        pass

    # übergeordnete Ressource
    f773 = xmlrecord.get("metadata").get("oai_dc:dc").get("dc:source", "")
    f773 = f773[0]
    f773g = f773.get("#text")

    regexp1 = re.search("Nr\.\s(\d.*?)\s\((\d\d\d\d)\);\s(\d+)", f773g)
    regexp2 = re.search(";\s(\d\d\d\d):\s(.*?);\s(\d+)", f773g)

    if regexp1:  
        issue = regexp1.group(1)
        year =  regexp1.group(2)
        startpage= regexp1.group(3)
        f773g = "(%s) Heft %s, S. %s-" % (year, issue, startpage)    
        marcrecord.add("773", g=f773g, t="MEDIENwissenschaft: Rezensionen | Reviews")
    elif regexp2:       
        year =  regexp2.group(1)
        issue = regexp2.group(2)
        startpage= regexp2.group(3)
        f773g = "(%s), %s, S. %s-" % (year, issue, startpage)    
        marcrecord.add("773", g=f773g, t="MEDIENwissenschaft: Rezensionen | Reviews")       
    else:
        print("Regexp konnte nicht gelesen werden: %s" % f773)
    
    # Link zur Ressource
    f856u = xmlrecord.get("metadata").get("oai_dc:dc").get("dc:relation", "")
    marcrecord.add("856", q="text/html", _3="Link zur Ressource", u=f856u)

    # Medienform
    marcrecord.add("935", b="cofz")

    # Kollektion
    # marcrecord.add("980", a=f001, b="73", c="Medienwissenschaft, Rezensionen, Reviews")
    marcrecord.add("980", a=f001, b="73", c="MedienwRezensionen")

    
    outputfile.write(marcrecord.as_marc())


inputfile.close()
outputfile.close()