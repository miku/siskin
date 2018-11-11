#!/usr/bin/env python3
# coding: utf-8

import io
import re
import sys

import xmltodict
import marcx


#Article</dc:type>
#Charter</dc:type>
#Fragment</dc:type>
#Letter</dc:type>
#Manuscript</dc:type>
#Map</dc:type>
#Monograph</dc:type>
#Multivolume_work</dc:
#Periodical</dc:type>
#Series</dc:type>
#Text</dc:type>
#Volume</dc:type>


inputfilename = "107_input.xml" 
outputfilename = "107_output.mrc"

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
    regexp = re.match("oai:digi.ub.uni-heidelberg.de:(\d+)", f001)
    if regexp:
        f001 = regexp.group(1)
        marcrecord.add("001", data="finc-107-" + f001)
    else:
        print("Der Identifier konnte nicht zerlegt werden: " + f001)

    # 007
    marcrecord.add("007", data="cr")

    # Sprache
    language = xmlrecord["metadata"]["oai_dc:dc"]["dc:language"]   
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
    f856u = xmlrecord["metadata"]["oai_dc:dc"]["dc:identifier"]
    if len(f856u) == 2:
        marcrecord.add("856", q="text/html", _3="Link zum Datensatz", u=f856u[0])  
        marcrecord.add("856", q="text/html", _3="Link zur Ressource", u=f856u[1])
    else:
        print("Die URLs weichen vom üblichen Schema ab: " + f001)

    # Medientyp
    marcrecord.add("935", b="cofz")
   
    # Kollektion
    # noch Reihenfolge herstellen
    marcrecord.add("980", a=f001, b="107", c="sid-107-col-heidelberg")

    
    outputfile.write(marcrecord.as_marc())


inputfile.close()
outputfile.close()