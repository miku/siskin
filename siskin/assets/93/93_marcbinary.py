#!/usr/bin/env python3
# coding: "utf-8"

import base64
import io
import os
import re
import sys

import html
import marcx


def get_field(field):

    pattern = "<%s.*?>(.*?)<\/%s>" % (field, field)

    regexp = re.search(pattern, line)
    if regexp:
        value = regexp.group(1)
        value = html.unescape(value)
        return value


inputfile = io.open("93_input.xml", "r", encoding="utf-8")
outputfile = io.open("93_output.mrc", "wb")

in_record = False

for i, line in enumerate(inputfile, start=0):

    if "<dc:title>" in line:
        f245a = get_field("dc:title")
    elif "<dc:creator>" in line:
        f100a = get_field("dc:creator")
    elif "<dc:publisher>" in line:
        f260b = get_field("dc:publisher")
    elif "<dc:date>" in line:
        f260c = get_field("dc:date")
    elif "<dc:source>" in line:
        f500a = get_field("dc:source")
        if f500a != None:
            regexp = re.search("\.\s(\w+)\s\d\d\d\d", f500a)
            if regexp:
                f260a = regexp.group(1)
            else:
                f260a = ""
    elif "<dc:identifier>" in line:
        identifier = get_field("dc:identifier")
        if identifier != None:
            if "http://www.zvdd" in identifier:
                f001 = identifier
                f001 = f001.encode("utf8")
                f001 = base64.b64encode(f001)
                f001 = f001.decode("ascii")
                f856u = identifier
    elif "</oai_dc:dc>" in line:
        marcrecord = marcx.Record(force_utf8=True)
        marcrecord.strict = False
        marcrecord.leader = "       b  22        450 "
        marcrecord.add("001", data="finc-93-%s" % f001)
        marcrecord.add("007", data="tu")
        marcrecord.add("100", a=f100a)
        marcrecord.add("245", a=f245a)
        marcrecord.add("260", a=f260a, b=f260b, c=f260c)
        marcrecord.add("500", a=f500a)
        marcrecord.add("856", q="text/html", _3="Link zur Ressource", u=f856u)
        marcrecord.add("980", a=f001, b="93", c="Zentrales Verzeichnis digitalisierter Drucke")

        outputfile.write(marcrecord.as_marc())

        f100a = None
        f245a = None
        f260a = None
        f260b = None
        f260c = None
        f500a = None
        f856u = None

        if i > 200000:
            break

    else:
        continue

inputfile.close()
outputfile.close()
