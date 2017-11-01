#/usr/local/env python
# coding: utf-8

import io
import sys
import json

import marcx

formatmap = {
    "Buch":
    {
        "leader": "nam",
        "007": "tu",
        "935b" : "druck"
    },
    "Zeitschrift":
    {
        "leader": "nas",
        "007": "tu",
        "935b" : "cofz"
    },
    "Dissertation":
    {
        "leader": "ckm",
        "007": "ta",        
        "935b" : "foto"
    },
    "Manuskript":
    {
        "leader": "ckm",
        "007": "ta",        
        "935b" : "foto"
    },
    "Bericht":
    {
        "leader": "ckm",
        "007": "ta",        
        "935b" : "foto"
    },
    "Wörterbuch":
    {
        "leader": "ckm",
        "007": "ta",        
        "935b" : "foto"
    },
    "Tagungsband":
    {
        "leader": "ckm",
        "007": "ta",        
        "935b" : "foto"
    },
    "Tagungsbericht":
    {
        "leader": "ckm",
        "007": "ta",        
        "935b" : "foto"
    },
    "Literaturzusammenstellung":
    {
        "leader": "ckm",
        "007": "ta",        
        "935b" : "foto"
    },
    "Konferenzbericht":
    {
        "leader": "ckm",
        "007": "ta",        
        "935b" : "foto"
    },
    "Diplomarbeit":
    {
        "leader": "ckm",
        "007": "ta",        
        "935b" : "foto"
    },
    "Software":
    {
        "leader": "ckm",
        "007": "ta",        
        "935b" : "foto"
    },
    "CD-ROM":
    {
        "leader": "ckm",
        "007": "ta",        
        "935b" : "foto"
    },
    "Karte":
    {
        "leader": "ckm",
        "007": "ta",        
        "935b" : "foto"
    },
    "Verzeichnis":
    {
        "leader": "ckm",
        "007": "ta",        
        "935b" : "foto"
    },
    "Datenbank":
    {
        "leader": "ckm",
        "007": "ta",        
        "935b" : "foto"
    },
    "Seminarvortrag":
    {
        "leader": "ckm",
        "007": "ta",        
        "935b" : "foto"
    },
    "Prospektmaterial":
    {
        "leader": "ckm",
        "007": "ta",        
        "935b" : "foto"
    },
    "Proceedings":
    {
        "leader": "ckm",
        "007": "ta",        
        "935b" : "foto"
    }
}

def get_leader(format="photograph"):
    return "     %s  22        450 " % formatmap[format]["leader"]

def get_field_007(format="photograph"):
    return formatmap[format]["007"]

def get_field_935b(format="photograph"):
    if "935b" not in formatmap[format]:
        return ""
    return formatmap[format]["935b"]

# Default input and output.
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

	# Leader
    leader = get_leader(format)
    marcrecord.leader = leader

    #Identifikator
    f001 = jsonrecord["ID"]
    f001 = f001.split("_")
    f001 = f001[1] 
    marcrecord.add("001", data="finc-131-" + f001)

    # Format (007)
    f007 = get_field_007(format)
    marcrecord.add("007", data=f007)
    
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

    # Schlagwörter
    substance = jsonrecord["SUBSTANCE"]
    keywords = jsonrecord["TOPIC_DETAILED"]
    
    if "Buch" in keywords:
    	keywords.remove("Buch")
    
    if "Zeitschrift" in keywords:
    	keywords.remove("Zeitschrift")
    
    for keyword in keywords:
    	marcrecord.add("689", a=keyword)    
 
    if substance not in keywords:
    	marcrecord.add("689", a=substance)

    if format not in keywords and format != "Buch" and format != "Zeitschrift":
        marcrecord.add("689", a=format)

    # weitere Schöpfer
    if len(authors) > 1:
        for f700a in authors[1:]:
            f700a = f700a.strip()    		
            if f700a != "u.a.":
                marcrecord.add("700", a=f700a)

    # Quelle
    f773t = jsonrecord["CONT_TITLE"] # wenn kein vollständiges f773g, ist f773t meist nur "Buch" oder "Beitrag"
    f773 = jsonrecord["VOL_ISSUE"]
    f773 = f773.split("/")   
    if len(f773) == 3:
    	volume = f773[0]
    	issue = f773[1]
    	pages = f773[2]
    	year = jsonrecord["YEAR"]
    	f773g = "%s(%s) Heft %s, S. %s" % (volume, year, issue, pages)
    else:
    	f773g = f773[0] # hier steht viel Murks, eventuell f773g = ""
    marcrecord.add("773", t=f773t, g=f773g)

    # Format (935b)
    f935b = get_field_935b(format)
    marcrecord.add("935", b=f935b)

    # Kollektion
    marcrecord.add("980", a=f001, b="131", c="gdmb")

    outputfile.write(marcrecord.as_marc())

outputfile.close()