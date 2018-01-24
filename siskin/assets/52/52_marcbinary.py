# /usr/bin/env python3
# coding: utf-8

import re
import sys

import pymarc
import marcx


copytags = ("003", "005", "006", "007", "008", "020", "022", "024", "035", "040",
            "084", "100", "110", "245", "246", "260", "300", "310", "362", "490",
            "505", "520", "650", "651", "700", "710", "760", "762", "773", "775",
            "780", "785", "830")


inputfilename = "52_input.mrc" 
outputfilename = "52_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = open(inputfilename, "rb")
outputfile = open(outputfilename, "wb")


reader = pymarc.MARCReader(inputfile)

for oldrecord in reader:

    newrecord = marcx.Record(force_utf8=True)

    # leader
    leader = "     " + oldrecord.leader[5:]
    newrecord.leader = leader

    # 001
    f001 = oldrecord["001"].data   
    f001 = f001.replace("-", "")
    f001 = f001.replace("_", "")
    print(f001)
    newrecord.add("001", data="finc-52-%s" % f001)

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
        f689a = oldrecord["653"]["a"]     
        newrecord.add("689", a=f689a)
    except (AttributeError, TypeError):
        pass

    # URL
    f856u = oldrecord["856"]["u"]
    newrecord.add("856", q="text/html", _3="Link zur Ressource", u=f856u)

	# 980
    collections = ["a", f001, "b", "52", "c", "OECD iLibrary"]
    newrecord.add("980", subfields=collections)
  
    outputfile.write(newrecord.as_marc())

inputfile.close()
outputfile.close()