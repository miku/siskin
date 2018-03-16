#!/usr/bin/env python
# coding: utf-8

from builtins import *

import io
import re
import sys

import marcx
import pymarc
from tqdm import tqdm

copytags = ["003", "005", "006", "008", "010", "013", "015", "016",
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

inputfilename, outputfilename = "117_input.mrc", "117_output.mrc"

if len(sys.argv) >= 3:
    inputfilename, outputfilename = sys.argv[1:3]

# Hidden parameter, total number of records, as a hint for tqdm.
total = None if len(sys.argv) < 4 else int(sys.argv[3])

inputfile = io.open(inputfilename, "rb")
outputfile = io.open(outputfilename, "wb")

reader = pymarc.MARCReader(inputfile)

# Filtering by RVK. There are other classification schemes used in 084.2, e.g. in the matched subset:
#
#  274772 rvk
#   34325 sdnb
#   12453 ssgn
#    9641 stub
#    9222 ifzs
#    1361 sbb
#     763 dopaed
#     406 fid
#     190 z
#      42 sfb
#       6 kab
#       5 ssd
#       5 ghbs
#       3 zdbs

# A plain whitelist.
whitelist = set([
    "AN 1780",
    "AN 3900",
    "AN 3920",
    "AN 4030",
    "CV 3500",
    "DW 4000",
    "DW 4200",
    "MF 1000",
    "MF 1500",
    "NQ 2270"
])

# Extra patterns.
pattern_ms = re.compile(r"^MS.7[89][56789].*$")
pattern_ap = re.compile(r"^AP.*$")
pattern_f2 = re.compile(r"^AP.99[012].*$")  # via: filter2_relevant_for_FID.xml

# Blacklist of signatures.
blacklist = set([
    "AP 6000",
    "AP 6300",
    "AP 6400",
    "AP 6500",
    "AP 6582",
    "AP 6583",
    "AP 6586",
    "AP 6600",
    "AP 6630",
    "AP 6800",
    "AP 6930",
    "AP 7200",
    "AP 7250",
    "AP 7320",
    "AP 7337",
    "AP 7900",
    "AP 8300",
    "AP 8735",
    "AP 8786",
    "AP 9950",
    "AP 9954"
])


def filter_084a(value):
    """
    A filter helper for a single 084a value. Returns True, if the value passes.
    """
    if value in whitelist:
        return True
    if pattern_ms.match(value):
        return True
    if pattern_ap.match(value) and value not in blacklist and not pattern_f2.match(value):
        return True
    return False


# Transformation.
for oldrecord in tqdm(reader, total=total):

    newrecord = marcx.Record()

    # via: filter_DE-B170.xml
    isils = set([s for f in oldrecord.get_fields("049")
                 for s in f.get_subfields("a")])

    if "DE-B170" not in isils:
        continue

    # prüfen, ob für adlr relevante RVK-Klasse
    rvk = [s for f in oldrecord.get_fields("084")
           for s in f.get_subfields("a")]
    for value in rvk:
        if filter_084a(value):
            break
    else:
        continue

    # prüfen, ob Titel vorhanden ist
    if not oldrecord["245"]:
        continue

    for field in oldrecord.get_fields("856"):
        f856 = field if "http" in field else ""

    # leader
    newrecord.leader = "     " + oldrecord.leader[5:]

    # 001
    f001 = oldrecord["001"].data
    newrecord.add("001", data="finc-117-%s" % f001)

    # 007
    if "007" in oldrecord:
        f007 = oldrecord["007"].data
        if len(f007) < 2:
            f007 = f007 + "u"
        newrecord.add("007", data=f007)

    # Originalfelder, die ohne Änderung übernommen werden
    for tag in copytags:
        for field in oldrecord.get_fields(tag):
            newrecord.add_field(field)

    # 980
    collections = ["a", f001, "b", "117", "c",
                   "UdK Berlin", "c", "Verbundkatalog Film"]
    newrecord.add("980", subfields=collections)

    outputfile.write(newrecord.as_marc())

inputfile.close()
outputfile.close()
