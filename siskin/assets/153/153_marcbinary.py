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
        f001 = f001.rstrip("=")
        marcrecord.add("001", data="finc-153-" + f001)        
    else:
        continue

    # Format
    marcrecord.add("007", data="cr")

    # Urheber
    try:
        f110a = xmlrecord["metadata"]["oai_dc:dc"]["dc:creator"]        
    except:
        f110a = ""

    if f110a != "" and f110a != "Unknown":
            marcrecord.add("110", a=f110a)
    
    # Hauptitel
    f245a = xmlrecord["metadata"]["oai_dc:dc"]["dc:title"]
    marcrecord.add("245", a=f245a)

    # Filmstudio
    try:
        f260b = xmlrecord["metadata"]["oai_dc:dc"]["dc:publisher"]        
    except:
        f260b = ""

    # Erscheinungsjahr
    try:
        f260c = xmlrecord["metadata"]["oai_dc:dc"]["dc:date"]        
    except:
        f260c = ""

    if f260c != "":
        regexp = re.search("(\d\d\d\d)", f260c)
        if regexp:
            f260c = regexp.group(1)

    # verhindert, dass Körperschaft und Verlag nebeneinander im Katalog angezeigt werden, wenn beide identisch sind 
    if f260b != "" and f260b == f110a:
        f260b = ""

    # ergänzt das Trennzeichen zwischen Produktionsfirma und Jahr, wenn beides vorhanden 
    if f260b != "" and f260c != "":
        f260b = f260b + ", "

    marcrecord.strict = False
    publisher = ["b", f260b, "c", f260c]
    marcrecord.add("260", subfields=publisher)
    marcrecord.strict = True

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
            if subjects != "need keyword":
                subject = subjects.title()
                marcrecord.add("650", a=subject)
         
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