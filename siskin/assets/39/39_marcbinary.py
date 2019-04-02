#!/usr/bin/env python
# coding: utf-8

from builtins import *

from io import StringIO, BytesIO
import io
import re
import sys
import pymarc

import marcx
from siskin.utils import xmlstream


inputfilename = "39_input.xml"
outputfilename = "39_output.mrc"
filterfilename = "39_issn_filter"

if len(sys.argv) >= 4:
    inputfilename, outputfilename, filterfilename = sys.argv[1:4]

outputfile = io.open(outputfilename, "wb")
filterfile = io.open(filterfilename, "r")

issn_list = filterfile.readlines()
issn_list = [issn.rstrip("\n") for issn in issn_list]

for record in xmlstream(inputfilename, "record"):
    record = BytesIO(record)
    record = pymarc.marcxml.parse_xml_to_array(record)
    record = record[0]
    
    record = marcx.Record.from_record(record)
    record.force_utf8 = True
    record.strict = False

    # prüfen, ob Titel vorhanden ist
    if not record["245"]:
        continue

    # Leader
    record.leader = "     " + record.leader[5:]

    # Identifikator   
    f001 = record["001"].data
    record.remove_fields("001")
    f001 = f001.replace("-", "").replace("_", "")
    record.add("001", data="finc-39-%s" % f001)

    # Kollektion und Ansigelung
    try:
        f022a = record["022"]["a"]
    except:
        f022a = ""

    try:
        f760x = record["760"]["x"]
    except:
        f760x = ""

    try:
         f787x = record["787"]["x"]
    except:
        f787x = ""      

    if f022a in issn_list or f760x in issn_list or f787x in issn_list:
        collections = ["a", f001, "b", "39", "c", u"sid-39-col-persee", "c", u"sid-39-col-perseeadlr"]
    else:
        collections = ["a", f001, "b", "39", "c", u"sid-39-col-persee"]

    record.add("980", subfields=collections)
    outputfile.write(record.as_marc())

outputfile.close()
filterfile.close()
