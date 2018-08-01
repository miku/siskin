#!/usr/bin/env python3
# coding: utf-8

import io
import sys
import re
import json

import marcx


formatmap = {
    "Buch": "Buch",
    "Konferenzbericht": "Buch",
    "Literaturzusammenstellung": "Buch",
    "Manuskript": "Buch",
    "Proceedings": "Buch",
    "Prospektmaterial": "Buch",
    "Seminarvortrag": "Buch",
    "Tagungsband": "Buch",
    "Verzeichnis": "Buch",
    "Wörterbuch": "Buch",
    "Bericht": "Buch",
    "Tagungsbericht": "Buch",
    "Diplomarbeit": "Hochschulschrift",
    "Dissertation": "Hochschulschrift",
    "Zeitschrift": "Artikel", # Zeitschriften sind hier eigentlich Zeitschriftenaufsätze
    "Artikel": "Artikel",
    "Aufsatz": "Artikel",
    "Aufsatz Kinderzeitschrift": "Artikel",
    "Rezension": "Artikel",
    "Verweisung": "Artikel",
    "Zeitschriftenartikel": "Artikel",
    "Zeitungsartikel": "Artikel",
    "Karte": "Karte",
    "Software": "Software",
    "CD-ROM": "Datenträger",
    "Datenbank": "Datenträger"
}


def getSubjects(tag):
    return jsonrecord[tag]


inputfilename = "131_input.json"
outputfilename = "131_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

with open(inputfilename, "r") as inputfile:
    jsonrecords = json.load(inputfile)

outputfile = io.open(outputfilename, "wb")

for jsonrecord in jsonrecords:

    if not jsonrecord["ID"] or not jsonrecord["TITLE"]:
        continue

    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False
    format = jsonrecord["FORMAT"]
   
    if formatmap[format] == "Buch":
       leader = "     nam  22        4500"
       f007 = "tu"
       f008 = ""
       f935b = "druck"
       f935c = ""
    elif formatmap[format] == "Hochschulschrift":
        leader = "     nam  22        4500"
        f007 = "tu"
        f008 = ""
        f935b = ""
        f935c = "hs"    
    elif formatmap[format] == "Artikel":
        leader = "     cab  22        4500"
        f007 = "tu"
        f008 = ""
        f935b = "SAXB"
        f935c = "druck"
    elif formatmap[format] == "Karte":
        leader = "     cem  22        4500"
        f007 = "au"
        f008 = ""
        f935b = "druck"
        f935c = "kart" 
    elif formatmap[format] == "Software":
        leader = "     cgm  22        4500"
        f007 = "co"
        f008 = ""
        f935b = "crom"
        f935c = "lo"
    elif formatmap[format] == "Datenträger":
        leader = "     cgm  22        4500"
        f007 = "v"
        f008 = ""
        f935b = "soerd"
        f935c = ""    
    else:
        print("Format %s ist nicht in der Mapping-Tabelle enthalten" % format)

    # Leader
    marcrecord.leader = leader

    #Identifikator
    f001 = jsonrecord["ID"]
    f001 = f001.split("_")
    f001 = f001[1] 
    marcrecord.add("001", data="finc-131-" + f001)

    # Format (007)
    marcrecord.add("007", data=f007)

    # Erscheinungsweise (008)
    marcrecord.add("008", data=f008)
    
    # ISBN
    f020a = jsonrecord["ISBN"]
    marcrecord.add("020", a=f020a)

    # 1. Schöpfer
    authors = jsonrecord["AUTHOR"]
    if authors != "N.N." and authors != "Autorenteam":
        authors = authors.split(";")
        f100a = authors[0]     
        marcrecord.add("100", a=f100a)     
    else:
        authors = ""

    # Haupttitel
    f245a = jsonrecord["TITLE"]
    marcrecord.add("245", a=f245a)
   
    # Erscheinungsvermerk   
    f260c = jsonrecord["YEAR"]
    marcrecord.add("260", c=f260c)

    # Seitenzahl    
    f300 = jsonrecord["VOL_ISSUE"]
    f300 = f300.split("/")
    if len(f300) == 3:     
        pages = f300[2]      
        regexp = re.search("\D1-(\d+)", pages)
        if regexp:
            f300a = regexp.group(1)
            f300a = f300a + " S."
        else:
            f300a = ""
        marcrecord.add("300", a=f300a)

    # Hochschulvermerk
    if formatmap[format] == "Hochschulschrift":
        f502a = jsonrecord["CONT_TITLE"]
        marcrecord.add("502", a=f502a)
   
    # Format (935bc)  
    marcrecord.add("935", b=f935b, c=f935c)

    # Schlagwörter
    keywords = getSubjects("TOPIC_DETAILED")  # ist immer eine Liste
    if "Buch" in keywords:
        keywords.remove("Buch")
    
    if "Zeitschrift" in keywords:
       keywords.remove("Zeitschrift")

    substances = getSubjects("SUBSTANCE")
    if "/" in substances:
        substances = substances.split("/")
    else:
        substances = substances.split(",")
    
    elements = getSubjects("ELEMENTS")
    if "/" in elements:
        elements = elements.split("/")
    else:
        elements = elements.split(",")
        
    for keyword in keywords:
        marcrecord.add("650", a=keyword.strip())
    
    if len(substances) == 2 and substances[1][0].islower():            
        substance = ", ".join(substances)
        marcrecord.add("650", a=substance.strip())
    else:
        for substance in substances:
            if substance not in keyword:
                marcrecord.add("650", a=substance.strip())
    
    if len(elements) == 2 and elements[1][0].islower():         
            element = ", ".join(elements)
            marcrecord.add("650", a=element.strip())
    else:
        for element in elements:
            if element not in keyword:
                marcrecord.add("650", a=element.strip())

    if format not in keywords and format != "Buch" and format != "Zeitschrift":
        marcrecord.add("650", a=format)

    # weitere Schöpfer
    if len(authors) > 1:
        for f700a in authors[1:]:
            f700a = f700a.strip()           
            if f700a != "u.a.":
                marcrecord.add("700", a=f700a)

    # Quelle
    f773t = jsonrecord["CONT_TITLE"] # wenn kein vollständiges f773g, ist f773t meist nur "Buch" oder "Beitrag"    
    f773 = jsonrecord["VOL_ISSUE"]
    if f773 == "" and f773t == "" and format == "Zeitschrift":       
        print("Der folgende Aufsatz hat keine übergordnete Zeitschrift:" + f001)
    f773 = f773.split("/")
    if len(f773) == 3:
        volume = f773[0]
        issue = f773[1]
        pages = f773[2]
        year = jsonrecord["YEAR"]
        f773g = "%s(%s) Heft %s, S. %s" % (volume, year, issue, pages)        
    else:
        f773g = f773[0] # hier steht viel Murks, eventuell f773g = ""
    if formatmap[format] == "Artikel":
        marcrecord.add("773", t=f773t, g=f773g)

    # Kollektion
    marcrecord.add("980", a=f001, b="131", c="GDMB")

    outputfile.write(marcrecord.as_marc())

outputfile.close()