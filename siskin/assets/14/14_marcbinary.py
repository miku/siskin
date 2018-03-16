#!/usr/bin/env python
# coding: utf-8

from builtins import *
import io
import sys

import pymarc
import marcx

copytags = ("003", "004", "005", "006", "008", "009", "010", "011", "012",
            "013", "014", "015", "016", "017", "018", "019", "020", "021", "022", "023",
            "024", "025", "026", "027", "028", "029", "030", "031", "032", "033", "034",
            "035", "036", "037", "038", "039", "040", "041", "042", "043", "044", "045",
            "046", "047", "048", "049", "050", "051", "052", "053", "054", "055", "056",
            "057", "058", "059", "060", "061", "062", "063", "064", "065", "066", "067",
            "068", "069", "070", "071", "072", "073", "074", "075", "076", "077", "078",
            "079", "080", "081", "082", "083", "085", "086", "087", "088", "089", "090",
            "091", "092", "093", "094", "095", "096", "097", "098", "099", "100", "101",
            "102", "103", "104", "105", "106", "107", "108", "110", "111", "112", "113",
            "114", "115", "116", "117", "118", "120", "121", "124", "125", "126", "129",
            "130", "131", "134", "135", "138", "142", "145", "148", "149", "151", "155",
            "156", "157", "160", "162", "163", "164", "165", "166", "167", "173", "174",
            "175", "176", "178", "183", "184", "187", "192", "200", "204", "210", "212",
            "234", "240", "243", "254", "256", "260", "300", "340", "383", "386", "387",
            "447", "448", "456", "490", "500", "504", "510", "518", "520", "541", "561",
            "563", "590", "591", "592", "593", "594", "595", "596", "597", "650", "657",
            "700", "710", "730", "762", "773", "775", "787")


def get_links(record):
    """
    Given a marc record, just return all links (from 856.u).

    Same as:

        >>> record = marcx.Record()
        >>> record.add('856', u='http://google.com')
        >>> list(record.itervalues('856.u'))
        ['http://google.com']

    """
    for field in record.get_fields("856"):
        for url in field.get_subfields("u"):
             yield url


def get_titles(record):
    for field in record.get_fields("772"):
        for title in field.get_subfields("a"):
            yield title

def get_field(record, field, subfield="a"):
    """
    Shortcut to get a record field and subfield or empty string, if no value is found.
    """
    try:
        value = record[field][subfield]
        return value if value is not None else ""
    except (KeyError, TypeError):
        return ""

# Default input and output.
inputfilename, outputfilename = "14_input.mrc", "14_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = io.open(inputfilename, "rb")
outputfile = io.open(outputfilename, "wb")

reader = pymarc.MARCReader(inputfile)

for oldrecord in reader:

    newrecord = marcx.Record()

    # prüfen, ob Titel vorhanden ist
    f245 = oldrecord["245"]
    if not f245:
        continue

    # prüfen, ob es sich um Digitalisat handelt
    f856 = oldrecord["856"]
    if not f856:
        continue

    for field in oldrecord.get_fields("856"):
        f856 = field if "http" in field else ""

    # leader
    leader = "     " + oldrecord.leader[5:]
    newrecord.leader = leader

    # 001
    f001 = oldrecord["001"].data
    newrecord.add("001", data="finc-14-%s" % f001)

    # Format 007
    newrecord.add("007", data="cr")

    # Originalfelder, die ohne Änderung übernommen werden
    for tag in copytags:
        for field in oldrecord.get_fields(tag):
            newrecord.add_field(field)

    # Haupttitel  
    f240a = get_field(oldrecord, "240", "a")
    f240m = get_field(oldrecord, "240", "m")
    f240n = get_field(oldrecord, "240", "n")
    f245a = "%s %s %s" % (f240a, f240m, f240n)
    newrecord.add("245", a=f245a)

    # Alternativtitel
    f246a = get_field(oldrecord, "245", "a")
    if f246a != "[without title]":
        newrecord.add("246", a=f246a)
    
    # Fußnote
    f852a = get_field(oldrecord, "852", "a")
    f852c = get_field(oldrecord, "852", "c") # häufig None
    f852x = get_field(oldrecord, "852", "x")
    f500a = "%s, %s, %s" % (f852a, f852c, f852x)
    newrecord.add("500", a=f500a)
    
    # enthaltene Werke
    try:      
        titles = list(get_titles(oldrecord))      
        if titles:          
            for title in titles:                      
                newrecord.add("505", a=title)
    except (KeyError, TypeError):
        pass

    # 856 (Digitalisat)
    links = list(get_links(oldrecord))
    if len(links) > 0:
        newrecord.add("856", q="text/html", _3="Link zum Digitalisat", u=links[0])

    # 856 (Datensatz)
    newrecord.add("856", q="text/html", _3="Link zum Datensatz", u="https://opac.rism.info/search?id=" + f001)

    # 970
    newrecord.add("970", c="PN")

    # 980
    newrecord.add("980", a=f001, b="14", c="RISM")

    outputfile.write(newrecord.as_marc())

inputfile.close()
outputfile.close()