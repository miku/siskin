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
            "206", "211", "236", "256", "260", "326", "363", "370", "377", "391", "411",
            "501", "506", "520", "542", "550", "600", "604", "650", "653", "690", "700",
            "710", "742", "760", "773", "777", "787", "797", "856", "860", "873",
            "880", "947", "948", "961"]


inputfilename = "39_input.xml"
outputfilename = "39_output.mrc"
filterfilename = "39_issn_filter"

if len(sys.argv) >= 4:
    inputfilename, outputfilename, filterfilename = sys.argv[1:4]

inputfile = io.open(inputfilename, "rb")
outputfile = io.open(outputfilename, "wb")
filterfile = io.open(filterfilename, "r")

reader = pymarc.parse_xml_to_array(inputfile)
issn_list = filterfile.readlines()
issn_list = [issn.rstrip("\n") for issn in issn_list]

for oldrecord in reader:

    newrecord = marcx.Record(force_utf8=True)
    newrecord.strict = False
    
    # prüfen, ob Titel vorhanden ist
    if not oldrecord["245"]:
        continue

    # Leader
    newrecord.leader = "     " + oldrecord.leader[5:]

    # Identifikator   
    f001 = oldrecord["001"].data
    f001 = f001.replace("-", "").replace("_", "")
    newrecord.add("001", data="finc-39-%s" % f001)
    
    # Originalfelder, die ohne Änderung übernommen werden
    for tag in copytags:
        for field in oldrecord.get_fields(tag):
            newrecord.add_field(field)

    # Titel
    # Wird extra verarbeitet, da Unterfelder teilweise leer sind und dann nicht angelegt werden sollen
    try:
        f245a = oldrecord["245"]["a"]
    except:
        f245a = ""

    try:
        f245b = oldrecord["245"]["b"]
    except:
        f245b = ""

    try:
        f245c = oldrecord["245"]["c"]
    except:
        f245c = ""

    try:
        f245h = oldrecord["245"]["h"]
    except:
        f245h = ""

    subfields = ["a", f245a, "b", f245b, "c", f245c, "h", f245h]
    newrecord.add("245", subfields=subfields)     

    # Kollektion und Ansigelung
    try:
        f022a = oldrecord["022"]["a"]
    except:
        f022a = ""

    try:
        f760x = oldrecord["760"]["x"]
    except:
        f760x = ""

    try:
         f787x = oldrecord["787"]["x"]
    except:
        f787x = ""      

    if f022a in issn_list or f760x in issn_list or f787x in issn_list:
        collections = ["a", f001, "b", "39", "c", u"sid-39-col-persee", "c", u"sid-39-col-perseeadlr"]
    else:
        collections = ["a", f001, "b", "39", "c", u"sid-39-col-persee"]

    newrecord.add("980", subfields=collections)
 
    outputfile.write(newrecord.as_marc())

inputfile.close()
outputfile.close()
filterfile.close()
