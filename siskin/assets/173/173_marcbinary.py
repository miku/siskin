# /usr/bin/env python3
# coding: utf-8

import re
import sys

import marcx
import pymarc
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
    newrecord.strict = False

    try:
        parent_type = oldrecord["773"]["7"]
    except:
        parent_type = ""

    # Leader
    if parent_type == "nnam":
        f935b = "druck"
        f935c = ""
        f008 = ""
        leader = "     " + oldrecord.leader[5:]
        newrecord.leader = leader
    elif parent_type == "nnas":
        f935b = "druck"
        f935c = "text"
        f008 = "                     p"
        leader = oldrecord.leader[8:]
        leader = "     nab" + leader
        newrecord.leader = leader
    else:
        f935b = "druck"
        f935c = ""
        f008 = ""
        leader = "     " + oldrecord.leader[5:]
        newrecord.leader = leader

    # Identifikator
    f001 = oldrecord["001"].data
    regexp = re.search("edoc/(.*)", f001)
    if regexp:
        f001 = regexp.group(1)
    else:
        sys.exit("Die ID konnte nicht verarbeitet werden: " + f001)
    f001 = f001.replace(":", "")
    newrecord.add("001", data="finc-173-%s" % f001)

    # Zugangsformat
    newrecord.add("007", data="tu")

    # Periodizität
    newrecord.add("008", data=f008)

    # Originalfelder, die ohne Änderung übernommen werden
    for tag in copytags:
        for field in oldrecord.get_fields(tag):
            newrecord.add_field(field)

    # SWB-Format
    newrecord.add("935", b=f935b, c=f935c)

    # 980
    collections = ["a", f001, "b", "173", "c", "sid-173-col-buchwesen"]
    newrecord.add("980", subfields=collections)

    marc_clean_record(newrecord)
    outputfile.write(newrecord.as_marc())

inputfile.close()
outputfile.close()
