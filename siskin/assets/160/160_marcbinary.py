#!/usr/bin/env python
# coding: utf-8

import io
import re
import sys
# https://stackoverflow.com/a/40846742/89391
import warnings

import pandas

import marcx
import pymarc

warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")

inputfilename = "160_input.csv"
outputfilename = "160_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

outputfile = io.open(outputfilename, "wb")
writer = pymarc.MARCWriter(outputfile)

csv_records = pandas.read_csv(inputfilename, encoding="latin-1", sep=";")
for csv_record in csv_records.iterrows():

    csv_record = csv_record[1]
    marc_record = marcx.Record(force_utf8=True)

    marc_record.leader = "     nam  22        4500"

    f001 = "finc-160-" + str(csv_record["001"])
    marc_record.add("001", data=f001)

    # Zugangsformat
    marc_record.add("007", data="tu")

    # Sprache
    marc_record.add("041", a=csv_record["041a"])

    # Notation
    marc_record.add("084", a="ZX 3900", _2="rvk")

    # 1. Urheber
    marc_record.add("100", a=csv_record["100a"])

    # Titel
    marc_record.add("245", a=csv_record["245a"])

    # Erscheinungsvermerk
    publisher = csv_record["502a"]
    match1 = re.search("(.*?),\s(.*?),\s(.*?),\s([\[\d].*)", publisher)
    match2 = re.search("(.*?),\s(.*?),\s(.*?),\s(.*?),\s([\[\d].*)", publisher)
    if match1:
        f260a = match1.group(1)
        f260b = match1.group(2)
    elif match2:
        f260a = match2.group(1)
        f260b1 = match2.group(2)
        f260b2 = match2.group(3)
        f260b = f260b1 + ", " + f260b2
    else:
        f260a = ""
        f260b = ""
    publisher = ["a", f260a, "b", f260b, "c", csv_record["260c"]]
    marc_record.add("260", subfields=publisher)

    # Umfang
    marc_record.add("300", a=csv_record["300a"])

    # Fußnote
    marc_record.add("500", a=u"Signatur: " + csv_record["Signatur\ngesamt"])

    # Hochschulvermerk
    university = csv_record["502a"]
    match1 = re.search("(.*?),\s(.*?),\s(.*?),\s([\[\d].*)", university)
    match2 = re.search("(.*?),\s(.*?),\s(.*?),\s(.*?),\s([\[\d].*)", university)
    if match1 or match2:
        marc_record.add("502", a=university)

    # weitere Urheber
    for field in csv_record.keys():
        if not field.startswith("700a"):
            continue
        if pandas.isnull(csv_record[field]):
            continue
        marc_record.add("700", a=csv_record[field])

    # Medienform
    marc_record.add("935", b=csv_record["935b"], c=csv_record["935c"])

    # Kollektion
    marc_record.add("980", a=str(csv_record["001"]), b="160", c="sid-160-col-diplspowi")

    writer.write(marc_record)

writer.close()
