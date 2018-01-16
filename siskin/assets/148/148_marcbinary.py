#!/usr/bin/env python
# coding: utf-8

from builtins import *
import io
import re
import sys

import pymarc
import marcx

copytags = ("002", "003", "004", "005", "006", "007", "008", "009", "010", "011", "012", "013", "014",
            "016", "017", "018", "022", "024", "030", "035", "040", "041", "100", "110", "111",
            "240", "242", "243", "245", "246", "247", "249", "250", "260", "263", "300", "310", "362",
            "490", "500", "501", "502", "504", "505", "510", "515", "516", "538", "546", "547", "550",
            "590", "600", "610", "611", "630", "648", "649", "651", "655", "700", "710", "711", "730",
            "770", "772", "773", "775", "776", "780", "785", "787", "800", "810", "811", "830", "856",
            "906", "982", "999")

# Default input and output.
inputfilename, outputfilename = "148_input.mrc", "148_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = io.open(inputfilename, "rb")
outputfile = io.open(outputfilename, "wb")

reader = pymarc.MARCReader(inputfile)

for oldrecord in reader:

    newrecord = marcx.Record(force_utf8=True)

    # prüfen, ob ID vorhanden ist
    if not oldrecord["001"]:      
        continue

    # prüfen ob Titel vorhanden ist
    if not (oldrecord["245"] and oldrecord["245"]["a"]):    
        continue

    # leader
    leader = "     " + oldrecord.leader[5:]
    newrecord.leader = leader

    # 001
    f001 = oldrecord["001"].data
    newrecord.add("001", data="finc-148-%s" % f001)

    # ISBN
    try:
        f020a = oldrecord["020"]["a"]        
    except:
        f020a = ""

    if f020a != "":
        f020a = f020a.replace(" ", "-")
        f020a = f020a.replace(".", "-")       
        regexp = re.search("([0-9xX-]{10,16})", f020a)
     
        if regexp:
            f020a = regexp.group(1)
            f020a = f020a.rstrip("-")
            newrecord.add("020", a=f020a) 
        else:
            print("Die ISBN %s konnte nicht mittels regulärer Ausdrücke überprüft werden." % f020a)

    # Originalfelder, die ohne Änderung übernommen werden
    for tag in copytags:
        for field in oldrecord.get_fields(tag):
            newrecord.add_field(field)

    # Schlagwort
    try:
        f689a = oldrecord["650"]["a"]     
        newrecord.add("689", a=f689a)
    except (AttributeError, TypeError):
        pass

    #n Link zum Datensatz
    newrecord.add("856", q="text/html", _3="Link zum Bundesarchiv", u="https://apps.bundesarchiv.de/F?func=find-c&ccl_term=SYS%3D" + f001 + "&local_base=BAB01")

    # 980
    collections = ["a", f001, "b", "148", "c", "Bundesarchiv (Filmarchiv)", "c", "Verbundkatalog Film"]
    newrecord.add("980", subfields=collections)
  
    outputfile.write(newrecord.as_marc())

inputfile.close()
outputfile.close()