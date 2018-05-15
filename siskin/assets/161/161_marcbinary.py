#!/usr/bin/env python3
# coding: utf-8

import re
import sys

import pymarc
import marcx


copytags = ("003", "005", "006", "007", "008", "020", "040",
             "043", "050", "082", "100", "245", "246", "250",
             "264", "300", "336", "337", "338", "490", "500",
             "505", "520", "600", "610", "650", "651", "700",
             "710", "776", "830", "856")


inputfilename = "161_input.mrc" 
outputfilename = "161_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = open(inputfilename, "rb")
outputfile = open(outputfilename, "wb")
reader = pymarc.MARCReader(inputfile)

for oldrecord in reader:

    try:
        f245a = oldrecord["245"]["a"]      
    except:      
        continue

    newrecord = marcx.Record(force_utf8=True)

    # Leader
    leader = "     " + oldrecord.leader[5:]
    newrecord.leader = leader

    # Identifikator
    f001 = oldrecord["001"].data
    f001 = str(f001)
    newrecord.add("001", data="finc-161-" + f001)

    # Originalfelder
    for tag in copytags:

        # ISBN
        if tag == "020":
        
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
                    print("Die ISBN %s konnte nicht mittels regulärer Ausdrücke überprüft werden." % f020a)

        else:
            for field in oldrecord.get_fields(tag):
                newrecord.add_field(field)   

    # Ansigelung
    collections = ["a", f001, "b", "161", "c", "sid-161-col-cambridgeopenaccess"]
    newrecord.add("980", subfields=collections)
   
    outputfile.write(newrecord.as_marc())

inputfile.close()
outputfile.close()