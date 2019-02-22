#!/usr/bin/env python3
# coding: utf-8


from siskin.mab import MabXMLFile

import io
import re
import sys

import xmltodict
import marcx


inputfilename = "142_input.xml" 
outputfilename = "142_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

records = MabXMLFile(inputfilename)
outputfile = open(outputfilename, "wb")

for record in records.records():
           
    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False

    # Format   
    format = record.field("050")

    if format == "a|a|||||||||||":
        leader = "     cam  22        4500"
        f935b = "druck"
        f935c = "lo"   
    elif format == "||a|||||d|||||":
        leader = "     naa  22        4500"
        f935b = "cofz"
        f935c = ""
    elif format == "||a||yy|d|||||":
        leader = "     cam  22        4500"
        f935b = "?"
        f935c = "?"
    elif format == "a|b|||||||||||":
        leader = "     cam  22        4500"
        f935b = "?"
        f935c = "?"
    elif format == "||a||ca|||||||":
        leader = "     cam  22        4500"
        f935b = "?"
        f935c = "?"
    else:
        leader = "     naa  22        4500"
        f935b = "?"
        f935c = ""

    # Leader
    marcrecord.leader = leader
      
    # Identifier
    f001 = record.field("001")
    f001 = "finc-142-" + f001
    marcrecord.add("001", data=f001)

    # 007
    marcrecord.add("007", data="tu")

    # ISBN
    isbns = record.fields("540")
    for f020a in isbns:
        f020a = f020a.replace("ISBN ", "")
        marcrecord.add("020", a=f020a)

    # ISSN
    issns = record.fields("542")
    for f022a in issns:
        f022a = f022a.replace("ISSN ", "")
        marcrecord.add("022", a=f022a)
    
    # Sprache
    f041a = record.field("037")
    if f041a:
        marcrecord.add("041", a=f041a)

    # 1. Schöpfer
    f100a = record.field("100")
    marcrecord.add("100", a=f100a)

    # 1. Körperschaft
    f110a = record.field("200")
    marcrecord.add("110", a=f110a)

    # Haupttitel
    f245a = record.field("331")
    if not f245a:
        continue
    
    f245b = record.field("335")
    f245c = record.field("359")
    subfields = ["a", f245a, "b", f245b, "c", f245c]
    marcrecord.add("245", subfields=subfields)
    
    # Erscheinungsvermerk
    f260a = record.field("410")       
    f260b = record.field("412")    
    f260c = record.field("425")
  
    subfields = ["a", f260a, "b", f260b, "c", f260c]
    marcrecord.add("260", subfields=subfields)

    # Umfangsangabe
    f300a = record.field("433")
    f300b = record.field("434")
    subfields = ["a", f300a, "b", f300b]
    marcrecord.add("300", subfields=subfields)
    
    # Reihe
 

    # Abstract
   

    # Schlagwörter
    

    # weitere geistige Schöpfer
    for i in range(104, 199, 4):
        tag = str(i)   
        f700a = record.field(tag)
        marcrecord.add("700", a=f700a)

    # weitere Körperschaften
    for i in range(204, 299, 4):
        tag = str(i)   
        f710a = record.field(tag)
        marcrecord.add("710", a=f710a)
    
    # übergeordnetes Werk
   
      
    # Link zu Datensatz und Ressource

    # Kollektion
    marcrecord.add("912", a="vkfilm")    
        
    # Medientyp
    marcrecord.add("935", b=f935b, c=f935c)
  
    # Kollektion    
    f001 = record.field("001")
    collections = ["a", f001, "b", "142", "c", "sid-142-col-gesamtkatduesseldorf"]
    marcrecord.add("980", subfields=collections)
      
    
    outputfile.write(marcrecord.as_marc())

outputfile.close()