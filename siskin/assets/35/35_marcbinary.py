#!/usr/bin/env python3
# coding: utf-8

# SID: 35
# Ticket: #14511, #15487
# TechnicalCollectionID: sid-35-col-hathi
# Task: TODO

from __future__ import print_function

import re
import sys
from io import BytesIO, StringIO

import marcx
import pymarc
from siskin.mappings import formats
from siskin.utils import marc_clean_record

setlist = ["hathitrust:pd"]

inputfilename = "35_input.xml"
outputfilename = "35_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = open(inputfilename, "r", encoding='utf-8')
outputfile = open(outputfilename, "wb")

pattern = re.compile(r"<setSpec>(.*?)</setSpec>.*<metadata>(.*)</metadata>")

for line in (line.strip() for line in inputfile if line.strip()):

    result = pattern.search(line)
    if not result:
        continue

    set, marc = result.groups()

    if set not in setlist:
        continue

    sio = StringIO(marc)
    parsed = pymarc.marcxml.parse_xml_to_array(sio)
    if len(parsed) != 1:
        raise ValueError("Der Record entspricht nicht genau einer Zeile: " + parsed)
    marcrecord = marcx.Record.from_record(parsed[0])
    marcrecord.strict = False

    # Leader
    marcrecord.leader = "     " + marcrecord.leader[5:]

    # Identifikator
    f001 = marcrecord["856"]["u"]
    match = re.search(".*\/(.*)", f001)
    if match:
        f001 = match.group(1)
        f001 = f001.replace(".", "")
        marcrecord.remove_fields("001")
        marcrecord.add("001", data="finc-35-%s" % f001)
    else:
        continue

    # Zugangsfacette
    marcrecord.add("007", data="cr")

    # Profilierung
    try:
        f050a = marcrecord["050"]["a"]
    except:
        continue

    if f050a:
        match = re.search("^M[LT][0-9]+.*$", f050a)
    if not match:
        continue

    collections = ["a", f001, "b", "35", "c", u"sid-35-col-hathi"]
    marcrecord.add("980", subfields=collections)

    marc_clean_record(marcrecord)
    outputfile.write(marcrecord.as_marc())

outputfile.close()
