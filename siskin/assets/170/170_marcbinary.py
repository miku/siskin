#!/usr/bin/env python3
# coding: utf-8

import io
import re
import sys

import xmltodict
import marcx


formatmaps = {
    '':
    {    
        'leader': '',
        '007': '',
        '008': '',
        '935b' : '',
        '935c' : ''
    },
    '':
    {    
        'leader': '',
        '007': '',
        '008': '',
        '935b' : '',
        '935c' : ''
    },
    '':
    {    
        'leader': '',
        '007': '',
        '008': '',
        '935b' : '',
        '935c' : ''
    },
    '':
    {    
        'leader': '',
        '007': '',
        '008': '',
        '935b' : '',
        '935c' : ''
    },
    '':
    {    
        'leader': '',
        '007': '',
        '008': '',
        '935b' : '',
        '935c' : ''
    },
    '':
    {    
        'leader': '',
        '007': '',
        '008': '',
        '935b' : '',
        '935c' : ''
    },
    '':
    {    
        'leader': '',
        '007': '',
        '008': '',
        '935b' : '',
        '935c' : ''
    }
}


inputfilename = "170_input.xml" 
outputfilename = "170_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = open(inputfilename, "rb")
outputfile = open(outputfilename, "wb")

xmlfile = inputfile.read()
xmlrecords = xmltodict.parse(xmlfile)

for xmlrecord in xmlrecords["Records"]["Record"]:
    
    if not xmlrecord["metadata"]["oai_dc:dc"].get("dc:title"):
        continue       
    
    marcrecord = marcx.Record(force_utf8=True)

    # Leader
    marcrecord.leader = "     cam  22        4500"
    
    # Identifier
    f001 = xmlrecord["header"]["identifier"]
    regexp = re.match("oai:mediarep.org:doc/(\d+)", f001)
    if regexp:
        f001 = regexp.group(1)
        marcrecord.add("001", data="finc-170-" + f001)
    else:
        print(u"Der Identifier konnte nicht zerlegt werden: " + f001)

    # 007
    marcrecord.add("007", data="cr")

    # Sprache
    language = xmlrecord["metadata"]["oai_dc:dc"]["dc:language"]
    if language == "de" or language == "deu":
        language = "ger"
    marcrecord.add("008", data="130227uu20uuuuuuxx uuup%s  c" % language)
    marcrecord.add("041", a=language)   

    # Verfasser
    if xmlrecord["metadata"]["oai_dc:dc"].get("dc:creator"):
        f100a = xmlrecord["metadata"]["oai_dc:dc"]["dc:creator"]
        marcrecord.add("100", a=f100a)

    # Titel   
    f245 = xmlrecord["metadata"]["oai_dc:dc"]["dc:title"]   
    marcrecord.add("245", a=f245)

    # Erscheinungsvermerk
    if xmlrecord["metadata"]["oai_dc:dc"].get("dc:date"):
        f260c = xmlrecord["metadata"]["oai_dc:dc"]["dc:date"]
        publisher = ["b", "Hochschule Mittweida,", "c", f260c]
        marcrecord.add("260", subfields=publisher)

    # Schlagwörter   
    if xmlrecord["metadata"]["oai_dc:dc"].get("dc:subject"):
        f689a = xmlrecord["metadata"]["oai_dc:dc"]["dc:subject"]   
        if isinstance(f689a, list):
            for subject in f689a:
                if ";" not in subject:
                    marcrecord.add("689", a=subject)
        else:          
            f689a = f689a.split(" , ")
            if len(f689a) > 1:
                for subject in f689a:
                    if ";" not in subject:
                        marcrecord.add("689", a=subject)

    # Link zu Datensatz und Ressource  
    urls = xmlrecord["metadata"]["oai_dc:dc"]["dc:identifier"]
    for f856u in urls:
        if "https://mediarep.org" in f856u:
            marcrecord.add("856", q="text/html", _3="Link zur Ressource", u=f856u)
            continue
    
    # Medientyp
    marcrecord.add("935", b="cofz")
   
    # Kollektion   
    collection = ["a", f001, "b", "170", "c", "sid-170-col-mediarep"]
    marcrecord.add("980", subfields=collection)

    
    outputfile.write(marcrecord.as_marc())


inputfile.close()
outputfile.close()
