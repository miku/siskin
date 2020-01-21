# /usr/bin/env python3
# coding: utf-8

import re
import sys

import marcx
import pymarc

copytags = ("003", "004", "005", "006", "007", "008", "009", "010", "011", "020", "090", "100", "240", "245", "246", "250", "260", "300", "490", "653", "700",
            "710", "773")

inputfilename = "155_input.mrc"
outputfilename = "155_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = open(inputfilename, "rb")
outputfile = open(outputfilename, "wb")

reader = pymarc.MARCReader(inputfile, force_utf8=True)

for oldrecord in reader:

    newrecord = marcx.Record(force_utf8=True)

    # leader
    leader = "     " + oldrecord.leader[5:]
    newrecord.leader = leader

    # 001
    f001 = oldrecord["001"].data
    f001 = f001.replace("*", "")
    f001 = f001.replace("-", "")
    newrecord.add("001", data="finc-155-%s" % f001)

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
    collections = ["a", f001, "b", "155", "c", "Deutsche Kinemathek (VK Film)"]
    newrecord.add("980", subfields=collections)

    outputfile.write(newrecord.as_marc())

inputfile.close()
outputfile.close()
