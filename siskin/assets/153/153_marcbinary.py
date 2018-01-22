# /usr/bin/env python
# coding: utf-8

import io
import re
import sys
import base64

import marcx
import xmltodict

# Default input and output.
inputfilename = "153_input.xml" 
outputfilename = "153_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = open(inputfilename, "rb")
outputfile = open(outputfilename, "wb")
xmlfile = inputfile.read()
xmlrecords = xmltodict.parse(xmlfile)

urls = []

for xmlrecord in xmlrecords["Records"]["Record"]:
    
    marcrecord = marcx.Record(force_utf8=True)

    # Leader
    marcrecord.leader = "     cam  22        4500"

    # Identifier
    try:
        f001 = xmlrecord["metadata"]["oai_dc:dc"]["dc:identifier"]       
    except:
        continue

    if f001 not in urls:
        urls.append(f001)
        f001 = f001.encode("utf-8")
        f001 = base64.b64encode(f001)
        f001 = f001.decode("ascii")
        marcrecord.add("001", data="finc-153-" + f001)        
    else:
        continue

    # Format
    marcrecord.add("007", data="cr")

    # Urheber
    try:
        f110a = xmlrecord["metadata"]["oai_dc:dc"]["dc:creator"]
        marcrecord.add("110", a=f110a)
    except:
        pass
    
    # Hauptitel
    f245a = xmlrecord["metadata"]["oai_dc:dc"]["dc:title"]
    if "Television Public Service" in f245a:
        continue
    else:
        marcrecord.add("245", a=f245a)

    # Filmstudio
    try:
        f260b = xmlrecord["metadata"]["oai_dc:dc"]["dc:publisher"]
        marcrecord.add("260", b=f260b)
    except:
        pass

    # Annotation
    try:
        f520a = xmlrecord["metadata"]["oai_dc:dc"]["dc:description"]
        marcrecord.add("520", a=f520a)
    except:
        pass

    # Schlagwörter
    subjects = xmlrecord["metadata"]["oai_dc:dc"].get("dc:subject", "")
    if subjects != "":
        if isinstance(subjects, list):
            for subject in subjects:
                subject = subject.title()
                marcrecord.add("650", a=subject)            
        else:
            subject = subject.title()
            marcrecord.add("650", a=subjects)
         
    # Link zur Ressource
    f856u = xmlrecord["metadata"]["oai_dc:dc"]["dc:identifier"]
    marcrecord.add("856", q="text/html", _3="Link zur Ressource", u=f856u)

    # Medienform
    marcrecord.add("935", b="cofz", c="vide")

    # Kollektion   
    marcrecord.add("980", a=f001, b="153", c="Internet Archive / Prelinger")


    outputfile.write(marcrecord.as_marc())

inputfile.close()
outputfile.close()