#!/usr/bin/env python
# coding: utf-8

from builtins import *

import io
import re
import sys

import marcx
import pymarc


copytags = ["005", "007", "008", "020", "024", "041", "042", "084", "093", "100",
            "110", "245", "246", "264", "300", "490", "500", "520", "540", "561",
            "650", "653", "700", "710", "773", "856"]

whitelist = set([
    "1080400",
    "10800",
    "1080401",
    "1080402",
    "1080403",
    "1080404",
    "1080405",
    "1080406",
    "1080407",
    "1080408",
    "1080409",
    "1080410",
    "1080411",
    "1080412",
    "10800",
    "1080400",   
    "10899"
])


collection_map = {
    "1080400": u"Massenkommunikation",
    "10800": u"Kommunikationswissenschaften",
    "1080401": u"Rundfunk, Telekommunikation",
    "1080402": u"Druckmedien",
    "1080403": u"Andere Medien",
    "1080404": u"Interaktive, elektronische Medien",
    "1080405": u"Medieninhalte, Aussagenforschung",
    "1080406": u"Kommunikatorforschung, Journalismus",
    "1080407": u"Wirkungsforschung, Rezipientenforschung",
    "1080408": u"Meinungsforschung",
    "1080409": u"Werbung, Public Relations, Öffentlichkeitsarbeit",
    "1080410": u"Medienpädagogik",
    "1080411": u"Medienpolitik, Informationspolitik, Medienrecht",
    "1080412": u"Medienökonomie, Medientechnik",
    "10800": u"Kommunikationswissenschaften",
    "1080400": u"Massenkommunikation (allgemein)",
    "10899": u"Sonstiges zu Kommunikationswissenschaften"
}


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


inputfilename = "30_input.xml"
outputfilename = "30_output.mrc"

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
    for identifiers in oldrecord.get_fields("024"):
        for f001 in identifiers.get_subfields("a"):
            regexp = re.search("document/(\d+)", f001)
            if regexp:
                f001 = regexp.group(1)
                newrecord.add("001", data="finc-30-%s" % f001)
                break
    else:
        print("Die ID konnte nicht verarbeitet werden: " + f001)

    # Originalfelder, die ohne Änderung übernommen werden
    for tag in copytags:
        for field in oldrecord.get_fields(tag):
            newrecord.add_field(field)

    # Reihen / Kollektion    
    for collections in oldrecord.get_fields("084"):
        for collection in collections.get_subfields("a"):
            f490a = collection_map.get(collection, "")
            newrecord.add("490", a=f490a)   

    # 980
    f980c = "sid-30-col-ssoar"
    for collections in oldrecord.get_fields("084"):
        for collection in collections.get_subfields("a"):          
            if collection in whitelist:
                f980c = "sid-30-col-ssoaradlr"
                break      
    newrecord.add("980", a="finc-30-" + f001, b="30", c=f980c)
 
    outputfile.write(newrecord.as_marc())

inputfile.close()
outputfile.close()
