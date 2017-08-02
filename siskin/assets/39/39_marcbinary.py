#!/usr/bin/dev python3
# coding: utf-8

import io
import re
import sys
import marcx
import base64

def get_field(field):

    if field.find("</") != -1:
        pattern = field
    else:
        pattern = "<%s.*?>(.*?)<\/%s>" % (field, field)

    regexp = re.search(pattern, line)
    if regexp:
       value = regexp.group(1)
       return value
    else:
       return ""

def get_repeatable_field(field):

    if field.find("</") != -1:
        pattern = field
    else:
        pattern = "(<%s.*?>.*<\/%s>)" % (field, field)

    regexp = re.search(pattern, line)
    if regexp:
       value = regexp.group(1)
       value = value.replace("<%s>" % field, "")
       value = value.split("</%s>" %field)
       return value
    else:
       return ""

inputfile = open("39_input.xml", "r", encoding="utf-8")
outputfile = open("39_output.mrc", "wb")

for i, line in enumerate(inputfile, start=1):

    line = line.replace("\n", "")

    f001 = get_field("identifier")
    if f001 == "":
        continue
    f001 = f001
    f001 = f001.encode("utf-8")
    f001 = base64.b64encode(f001)
    f001 = f001.decode("ascii")
    f001 = f001.rstrip("=")

    f022a = get_field("<dc:identifier scheme=\"ISSN\">issn:(.*)</dc:identifier>")

    f041a = get_field("<dc:language>(.*)</dc:language>")

    f100a =  get_repeatable_field("dc:creator")

    f245a = get_field("dc:title")
    try:
        f245a, f245b = f245a.split(" : ")
    except:
        f245b = ""

    f260b = get_field("dc:publisher")
    try:
        f260a, f260b = f260b.split(" : ")
    except:
        f260a = ""

    f260c = get_field("dc:date")

    f300a = get_field("dcterms:extent")
    f300a = f300a
    if f300a != "":
        regexp = re.search("(\d+)\D", f300a)
        if regexp:
            f300a = regexp.group(1)
    else:
        f300a = ""

    f500a = get_field("dcterms:bibliographicCitation")

    f520a = get_field("<dc:description xml:lang=\"\w\w\w\">(.*?)</dc:description>")

    f773a = get_field("dcterms:bibliographicCitation.jtitle")
    volume = get_field("dcterms:bibliographicCitation.volume")
    issue = get_field("dcterms:bibliographicCitation.issue")
    spage = get_field("dcterms:bibliographicCitation.spage")
    epage = get_field("dcterms:bibliographicCitation.epage")
    f773g = "%s(%s)%s, S. %s-%s" % (volume, f260c, issue, spage, epage)
    f773x = get_field("<dc:identifier scheme=\"ISSN\">issn:(.*)</dc:identifier>")

    f856u = get_field("dc:identifier")
    doi = get_field("<dc:identifier scheme=\"DOI\">(.*?)</dc:identifier>")

    f950a = get_field("<dc:subject xml:lang=\"\w\w\w\">(.*?)</dc:subject>")

    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False
    marcrecord.leader =  "       b  22        450 "
    marcrecord.add("001", data="finc-39-%s" % f001)
    marcrecord.add("007", data="tu")
    marcrecord.add("008", data="130227uu20uuuuuuxx uuup%s  c" % f041a)
    marcrecord.add("022", a=f022a)
    marcrecord.add("041", a=f041a)
    marcrecord.add("100", a=f100a)
    marcrecord.add("245", a=f245a, b=f245b)
    marcrecord.add("260", a=f260a, b=f260b, c=f260c)
    marcrecord.add("300", a=f300a)
    marcrecord.add("500", a=f500a)
    marcrecord.add("520", a=f520a)

    for author in f100a[1:]:
        marcrecord.add("700", a=author)

    marcrecord.add("773", a=f773a, g=f773g, x=f773x)
    marcrecord.add("856", q="text/html", _3="Link zur Ressource", u=[f856u, doi])
    marcrecord.add("950", a=f950a)
    marcrecord.add("980", a=f001, b="39")

    outputfile.write(marcrecord.as_marc())

    if i > 20000:
        break

inputfile.close()
outputfile.close()