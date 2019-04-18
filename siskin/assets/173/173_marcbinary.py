# /usr/bin/env python3
# coding: utf-8

import re
import re
import sys

import pymarc
import marcx

from siskin.utils import marc_clean_record


copytags = ("003", "084", "100", "245", "260", "300", "490", "553", "653", "773")

inputfilename = "173_input.mrc" 
outputfilename = "173_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = open(inputfilename, "rb")
outputfile = open(outputfilename, "wb")

reader = pymarc.MARCReader(inputfile, force_utf8=True)

for oldrecord in reader:

    newrecord = marcx.Record(force_utf8=True)

    # Leader
    leader = "     " + oldrecord.leader[5:]
    newrecord.leader = leader

    # Identifikator
    f001 = oldrecord["001"].data
    regexp = re.search("edoc/(.*)", f001)
    if regexp:
        f001 = regexp.group(1)
    else:
        print("Die ID konnte nicht verarbeitet werden: " + f001)
        sys.exit()
    f001 = f001.replace(":", "")
    newrecord.add("001", data="finc-173-%s" % f001)
   
    # Originalfelder, die ohne Änderung übernommen werden
    for tag in copytags:
        for field in oldrecord.get_fields(tag):
            newrecord.add_field(field)

	# 980
    collections = ["a", f001, "b", "173", "c", "sid-173-col-buchwesen"]
    newrecord.add("980", subfields=collections)
  
    marc_clean_record(newrecord)
    outputfile.write(newrecord.as_marc())

inputfile.close()
outputfile.close()
