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
    format = xmlrecord["metadata"]["oai_dc:dc"]["dc:type"]
    if format[0] == "article":
        marcrecord.leader = "     nab  22        4500"
    else:
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

    # ISBN
    if xmlrecord["metadata"]["oai_dc:dc"].get("dc:identifier"):
        identifiers = xmlrecord["metadata"]["oai_dc:dc"]["dc:identifier"]   
        if isinstance(identifiers, list):
            for identifier in identifiers:
                if re.search("([0-9xX-]{10,17})", identifier):
                    marcrecord.add("020", a=identifier)
                    break

    # ISSN
    if xmlrecord["metadata"]["oai_dc:dc"].get("dc:identifier"):
        identifiers = xmlrecord["metadata"]["oai_dc:dc"]["dc:identifier"]   
        if isinstance(identifiers, list):
            for identifier in identifiers:
                identifier = identifier.replace("issn:", "")
                if re.search("\d\d\d\d-\d\d\d\d$", identifier):
                    marcrecord.add("022", a=identifier)
                    break

    # Sprache
    language = xmlrecord["metadata"]["oai_dc:dc"]["dc:language"]
    if language == "de" or language == "deu":
        language = "ger"
    marcrecord.add("008", data="130227uu20uuuuuuxx uuup%s  c" % language)
    marcrecord.add("041", a=language)

    # Fachgebiet
    if xmlrecord["metadata"]["oai_dc:dc"].get("dc:subject"):
        subjects = xmlrecord["metadata"]["oai_dc:dc"]["dc:subject"]   
        if isinstance(subjects, list):
            for subject in subjects:
                if re.search("\d\d\d", subject):
                    marcrecord.add("082", a=subject, _2="ddc")
                    break
        else:
            if re.search("\d\d\d", subjects):
                marcrecord.add("084", a=subjects, _2="ddc")

    # 1. Urheber
    if xmlrecord["metadata"]["oai_dc:dc"].get("dc:creator"):
        creator = xmlrecord["metadata"]["oai_dc:dc"]["dc:creator"]
        if isinstance(creator, list):
            marcrecord.add("100", a=creator[0])
        else:
            marcrecord.add("100", a=creator)

    # Titel   
    f245 = xmlrecord["metadata"]["oai_dc:dc"]["dc:title"]   
    marcrecord.add("245", a=f245)

    # Erscheinungsvermerk
    if xmlrecord["metadata"]["oai_dc:dc"].get("dc:publisher"):
        f260b = xmlrecord["metadata"]["oai_dc:dc"]["dc:publisher"]
    else:
        f260b = ""

    if xmlrecord["metadata"]["oai_dc:dc"].get("dc:date"):
        f260c = xmlrecord["metadata"]["oai_dc:dc"]["dc:date"]
    else:
        f260c = ""

    publisher = ["b", f260b, "c", f260c]
    marcrecord.add("260", subfields=publisher)    

    # Beschreibung
    if xmlrecord["metadata"]["oai_dc:dc"].get("dc:description"):
        f520a = xmlrecord["metadata"]["oai_dc:dc"]["dc:description"]
        marcrecord.add("520", a=f520a)

    # Schlagwörter   
    if xmlrecord["metadata"]["oai_dc:dc"].get("dc:subject"):
        subjects = xmlrecord["metadata"]["oai_dc:dc"]["dc:subject"]   
        if isinstance(subjects, list):
            for subject in subjects:
                if not re.search("\d\d\d", subject):
                    marcrecord.add("650", a=subject)
        else:
            if not re.search("\d\d\d", subject):
                marcrecord.add("650", a=subject)

    # weitere Urheber
    if xmlrecord["metadata"]["oai_dc:dc"].get("dc:creator"):
        creators = xmlrecord["metadata"]["oai_dc:dc"]["dc:creator"]
        if isinstance(creators, list):
            for creator in creators[1:]:
                marcrecord.add("700", a=creator)

    # Zeitschrift
    sources = xmlrecord["metadata"]["oai_dc:dc"]["dc:source"]
    if isinstance(sources, list):
        for source in sources:
            if ":" in source:
                f773a = source.split("In: ")
                if len(f773a) == 2:
                    marcrecord.add("773", a=f773a[1])
                else:
                    marcrecord.add("773", a=f773a[0])
    else:
        f773a = sources.split("In: ")
        if len(f773a) == 2:
            marcrecord.add("773", a=f773a[1])
        else:
            marcrecord.add("773", a=f773a[0])


    # Link zu Datensatz und Ressource  
    urls = xmlrecord["metadata"]["oai_dc:dc"]["dc:identifier"]
    for f856u in urls:
        if "https://mediarep.org" in f856u:
            marcrecord.add("856", q="text/html", _3="Link zur Ressource", u=f856u)
        elif "doi.org" in f856u:
            marcrecord.add("856", q="text/html", _3="Zitierlink (DOI)", u=f856u)
            
    # Medientyp
    marcrecord.add("935", b="cofz")
   
    # Kollektion   
    collection = ["a", f001, "b", "170", "c", "sid-170-col-mediarep"]
    marcrecord.add("980", subfields=collection)

    
    outputfile.write(marcrecord.as_marc())


inputfile.close()
outputfile.close()
