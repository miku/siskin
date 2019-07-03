#!/usr/bin/env python
# coding: utf-8

import io
import logging
import re
import sys
from builtins import *
from io import BytesIO, StringIO

import marcx
import pymarc
from siskin.utils import marc_clean_record, xmlstream

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

    descriptions = list(record.itervalues('520.a'))
    record.remove_fields("520")

    # Ugly, but helps to emit valid binary MARC.
    descriptions = [d[:8000] for d in descriptions]
    total = sum((len(s) for s in descriptions))
    if total > 9000:
        logging.warn('%s: sum of descriptions exceeds (%d) limit, keeping 1 of %d fields', f001, total,
                     len(descriptions))
        descriptions = descriptions[:1]
    for description in descriptions:
        record.add("520", a=description)

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

    marc_clean_record(record)
    outputfile.write(record.as_marc())

outputfile.close()
filterfile.close()
