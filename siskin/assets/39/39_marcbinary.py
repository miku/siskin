#!/usr/bin/env python
# coding: utf-8

from builtins import *

import io
import re
import sys

import marcx
import pymarc


copytags = ["001", "003", "005", "007", "008", "022", "024", "040", "041", "100", "102",
            "108", "110", "112", "115", "120", "123", "124", "130", "137", "139", "140",
            "144", "146", "150", "151", "152", "159", "160", "166", "174", "175", "177",
            "178", "179", "180", "181", "182", "183", "184", "185", "186", "187", "189",
            "190", "191", "192", "193", "194", "195", "196", "197", "198", "199", "200",
            "206", "211", "236", "245", "256", "260", "326", "363", "370", "377", "391",
            "411", "501", "506", "520", "542", "550", "600", "604", "650", "653", "690",
            "700", "710", "742", "760", "773", "777", "787", "797", "856", "860", "873",
            "880", "947", "948", "961"]


inputfilename = "39_input.xml"
outputfilename = "39_output.mrc"

if len(sys.argv) >= 3:
    inputfilename, outputfilename = sys.argv[1:3]

inputfile = io.open(inputfilename, "rb")
outputfile = io.open(outputfilename, "wb")

reader = pymarc.parse_xml_to_array(inputfile)

for oldrecord in reader:

    newrecord = marcx.Record()
    newrecord.strict = False
    
    # prüfen, ob Titel vorhanden ist
    if not oldrecord["245"]:
        continue

    # leader
    newrecord.leader = "     " + oldrecord.leader[5:]

    # 001   
    f001 = oldrecord["001"].data
    newrecord.add("001", data="finc-39-%s" % f001)
    
    # Originalfelder, die ohne Änderung übernommen werden
    for tag in copytags:
        for field in oldrecord.get_fields(tag):
            newrecord.add_field(field)      

    # 980
    collections = ["a", f001, "b", "39", "c", u"sid-39-col-persee", "c", u"sid-39-col-perseeadlr"]
    newrecord.add("980", subfields=collections)
 
    outputfile.write(newrecord.as_marc())

inputfile.close()
outputfile.close()
