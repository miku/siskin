# /usr/bin/env python3
# coding: utf-8

import re
import sys

import pymarc
import marcx


copytags = ("003", "004", "005", "006", "007", "008", "009", "010", "011",
            "012", "013", "014", "015", "016", "017", "018", "019", "020",
            "021", "022", "023", "024", "025", "026", "027", "028", "029",
            "030", "031", "032", "033", "034", "035", "036", "037", "038",
            "039", "040", "041", "042", "043", "044", "045", "046", "047",
            "048", "049", "050", "051", "052", "053", "054", "055", "056",
            "057", "058", "059", "060", "061", "062", "063", "064", "065",
            "067", "069", "071", "072", "073", "075", "076", "077", "078",
            "079", "080", "082", "084", "085", "088", "089", "090", "091",
            "093", "094", "100", "103", "106", "107", "110", "111", "133",
            "141", "245", "246", "260", "300", "310", "362", "490", "505",
            "520", "650", "651", "700", "710", "760", "762", "773", "775",
            "780", "785", "830", "856")


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

	# 980
    collections = ["a", f001, "b", "52", "c", "OECD iLibrary"]
    newrecord.add("980", subfields=collections)
  
    outputfile.write(newrecord.as_marc())

inputfile.close()
outputfile.close()