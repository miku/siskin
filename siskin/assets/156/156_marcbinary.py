#!/usr/bin/env python3
# coding: utf-8

from __future__ import print_function

import re
import sys

import pymarc
import marcx


copytags = ("100", "105", "120", "130", "150", "174", "200", "245", "246", "250",
            "260", "300", "335", "336", "337", "338", "351", "361", "400", "500",
            "520", "650", "689", "700", "710", "800")


inputfilename = "156_input.xml" 
outputfilename = "156_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = open(inputfilename, "rb")
outputfile = open(outputfilename, "wb")
oldrecords = pymarc.parse_xml_to_array(inputfile)

for i, oldrecord in enumerate(oldrecords, start=1):

    try:
        f245a = oldrecord["245"]["a"]      
    except:      
        continue

    newrecord = marcx.Record(force_utf8=True)

    # leader
    leader = "     " + oldrecord.leader[5:]
    newrecord.leader = leader

    # 001
    f001 = str(i)
    newrecord.add("001", data="finc-156-" + f001)

    # ISBN
    try:
        f020a = oldrecord["020"]["a"]        
    except:
        f020a = ""

    if f020a != "":
        f020a = f020a.replace(" ", "-")
        f020a = f020a.replace(".", "-")       
        regexp = re.search("([0-9xX-]{10,17})", f020a)
     
        if regexp:
            f020a = regexp.group(1)
            f020a = f020a.rstrip("-")
            newrecord.add("020", a=f020a) 
        else:
            print(u"Die ISBN %s konnte nicht mittels regulärer Ausdrücke überprüft werden." % f020a, file=sys.stderr)

    # ISSN
    try:
        f022a = oldrecord["022"]["a"]        
    except:
        f022a = ""

    if f022a != "":
        f022a = f022a.replace(" ", "-")
        f022a = f022a.replace(".", "-")       
        regexp = re.search("([0-9xX-]{9})", f022a)
     
        if regexp:
            f022a = regexp.group(1)
            f022a = f022a.rstrip("-")
            newrecord.add("022", a=f022a) 
        else:
            print(u"Die ISSN %s konnte nicht mittels regulärer Ausdrücke überprüft werden." % f022a, file=sys.stderr)

    # Originalfelder, die ohne Änderung übernommen werden
    for tag in copytags:
        for field in oldrecord.get_fields(tag):
            newrecord.add_field(field)   

    # 980
    collections = ["a", f001, "b", "156", "c", "Umweltbibliothek Leipzig"]
    newrecord.add("980", subfields=collections)
   
    outputfile.write(newrecord.as_marc())

inputfile.close()
outputfile.close()
