#!/usr/bin/env python
# coding: utf-8

from builtins import *
import io
import sys

import pymarc
import marcx
from tqdm import tqdm

copytags = ["003", "005", "006", "007", "008", "010", "013", "015", "016",
            "017", "020", "022", "024", "026", "028", "029", "030", "032", "033", "035",
            "040", "041", "044", "045", "049", "050", "080", "082", "084", "086", "088",
            "100", "110", "111", "130", "210", "240", "242", "243", "245", "246", "247",
            "249", "250", "255", "259", "260", "264", "270", "300", "303", "336", "337",
            "338", "340", "344", "362", "363", "382", "385", "490", "500", "501", "502",
            "505", "511", "515", "518", "520", "530", "533", "534", "535", "538", "539",
            "546", "547", "555", "581", "583", "600", "610", "611", "630", "648", "650",
            "651", "653", "655", "689", "700", "710", "711", "730", "740", "751", "770",
            "772", "773", "775", "776", "777", "780", "785", "787", "800", "810", "830",
            "850", "856", "880", "912", "924", "940", "999"]


def get_links(record):
    for field in record.get_fields("856"):
        for url in field.get_subfields("u"):
            yield url


def get_titles(record):
    for field in record.get_fields("772"):
        for title in field.get_subfields("a"):
            yield title


def get_field(record, field, subfield="a"):
    try:
        value = record[field][subfield]
        return value if value is not None else ""
    except (KeyError, TypeError):
        return ""

inputfilename, outputfilename = "117_input.mrc", "117_output.mrc"

if len(sys.argv) >= 3:
    inputfilename, outputfilename = sys.argv[1:3]

# Hidden parameter, total number of records, as a hint for tqdm.
total = None
if len(sys.argv) >= 4:
    total = int(sys.argv[3])

inputfile = io.open(inputfilename, "rb")
outputfile = io.open(outputfilename, "wb")

reader = pymarc.MARCReader(inputfile)

whitelist = set(["AN 1780", "AN 3900", "AN 3920", "AN 4030", "CV 3500", "DW 4000", "DW 4200", "MF 1000", "MF 1500", "NQ 2270"])

for oldrecord in tqdm(reader, total=total):

    newrecord = marcx.Record()

    # prüfen, ob für adlr relevante RVK-Klasse
    # r = marcx.Record.from_record(oldrecord)
    # for value in r.itervalues("084.a"): ...
    rvk = [s for f in oldrecord.get_fields("084") for s in f.get_subfields("a")]
    for value in rvk:
        if value in whitelist:
            # print("%s whitelisted (%s), ok" % (oldrecord["001"].data, value))
            break
    else:
        # print("%s not whitelisted, skipping" % oldrecord["001"].data)
        continue

    # prüfen, ob Titel vorhanden ist
    f245 = oldrecord["245"]
    if not f245:
        continue

    for field in oldrecord.get_fields("856"):
        f856 = field if "http" in field else ""

    # leader
    leader = "     " + oldrecord.leader[5:]
    newrecord.leader = leader

    # 001
    f001 = oldrecord["001"].data
    newrecord.add("001", data="finc-117-%s" % f001)

    # Originalfelder, die ohne Änderung übernommen werden
    for tag in copytags:
        for field in oldrecord.get_fields(tag):
            newrecord.add_field(field)

    # 980
    collections = ["a", f001, "b", "117", "c", "UdK Berlin", "c", "Verbundkatalog Film"]
    newrecord.add("980", subfields=collections)

    outputfile.write(newrecord.as_marc())

inputfile.close()
outputfile.close()
