#!/usr/bin/dev python3
# coding: utf-8

from __future__ import print_function

import base64
import io
import re
import sys

import marcx


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
        value = value.split("</%s>" % field)
    else:
        value = []
    return value

input, output = "39_input.xml", "39_output.mrc"

if len(sys.argv) == 3:
    input, output = sys.argv[1], sys.argv[2]

inputfile = io.open(input, "r", encoding="utf-8")
outputfile = io.open(output, "wb")

for i, line in enumerate(inputfile, start=1):

    line = line.replace("\n", "")

    # Zugangsrechte
    if "<dcterms:accessRights>restricted</dcterms:accessRights>" in line:
        continue

    # Identfier
    f001 = get_field("identifier")
    if f001 == "":
        continue
    f001 = f001
    f001 = f001.encode("utf-8")
    f001 = base64.b64encode(f001)
    f001 = f001.decode("ascii")
    f001 = f001.rstrip("=")

    # ISSN
    f022a = get_field("<dc:identifier scheme=\"ISSN\">issn:(.*)</dc:identifier>")

    # Sprache
    f041a = get_field("<dc:language>(.*)</dc:language>")

    # Person
    persons = get_repeatable_field("dc:creator")

    # Titel
    f245a = get_field("dc:title")
    try:
        f245a, f245b = f245a.split(" : ")
    except:
        f245b = ""

    # Erscheinungsvermerk
    f260b = get_field("dc:publisher")
    try:
        f260a, f260b = f260b.split(" : ")
    except:
        f260a = ""
    f260c = get_field("dc:date")

    # Umfang
    f300a = get_field("dcterms:extent")
    f300a = f300a
    if f300a != "":
        regexp = re.search("(\d+)\D", f300a)
        if regexp:
            f300a = regexp.group(1)
    else:
        f300a = ""

    # ausführliche Quellenangabe
    f500a = get_field("dcterms:bibliographicCitation")

    # beschreibender Text
    f520a = get_field("<dc:description xml:lang=\"\w\w\w\">(.*?)</dc:description>")

    FIELD_LENGTH_LIMIT = 8000  # Limit is 9999 bytes, but as we might not only have ASCII, add a margin.

    if len(f520a) > FIELD_LENGTH_LIMIT:
        print("cutting off long description (%s -> %s) for %s" % (len(f520a), FIELD_LENGTH_LIMIT, get_field("identifier")), file=sys.stderr)
        f520a = f520a[:FIELD_LENGTH_LIMIT]  # Cut off long abstracts, refs #11349.

    format = get_field("dc:type")
    if format == "notebiblio":
        leader = "     naa  22        4500"
    elif format == "illustration":
        leader = "     ckm  22        4500"
    elif format == "liminaire":
        leader = "     naa  22        4500"
    elif format == "table":
        leader = "     naa  22        4500" 
    elif format == "book":
        leader = "     cam  22        4500"   
    else:
        leader = "     cab  22        4500"

    # Quelle
    f773a = get_field("dcterms:bibliographicCitation\.jtitle")
    volume = get_field("dcterms:bibliographicCitation\.volume")
    issue = get_field("dcterms:bibliographicCitation\.issue")
    spage = get_field("dcterms:bibliographicCitation\.spage")
    epage = get_field("dcterms:bibliographicCitation\.epage")
    f773g = "%s(%s)%s, S. %s-%s" % (volume, f260c, issue, spage, epage)
    f773x = get_field("<dc:identifier scheme=\"ISSN\">issn:(.*)</dc:identifier>")

    # URL und DOI
    f856u = get_field("dc:identifier")
    doi = get_field("<dc:identifier scheme=\"DOI\">(.*?)</dc:identifier>")

    # Schlagwörter
    f950a = get_field("<dc:subject xml:lang=\"\w\w\w\">(.*?)</dc:subject>")
    f950a = f950a.split(" ; ")

    marcrecord = marcx.Record(force_utf8=True, to_unicode=True)
    marcrecord.strict = False
    marcrecord.leader = leader
    marcrecord.add("001", data="finc-39-%s" % f001)
    marcrecord.add("007", data="cr")
    #marcrecord.add("008", data=f008)
    marcrecord.add("022", a=f022a)
    marcrecord.add("041", a=f041a)

    if len(persons) > 0:
        f100a = persons[0]
    else:
        f100a = ""

    marcrecord.add("100", a=f100a)
    marcrecord.add("245", subfields=["a", f245a, "b", f245b])
    marcrecord.add("260", a=f260a, b=f260b, c=f260c)
    marcrecord.add("300", a=f300a)
    marcrecord.add("500", a=f500a)
    marcrecord.add("520", a=f520a)

    if len(persons) > 1:
        for f700a in persons[1:]:
            marcrecord.add("700", a=f700a)

    marcrecord.add("773", a=f773a, g=f773g, x=f773x)
    marcrecord.add("856", q="text/html", _3="Link zur Ressource", u=f856u)

    if doi != "":
        marcrecord.add("856", q="text/html", _3="DOI", u="http://doi.org/" + doi)

    for subject in f950a:
        marcrecord.add("950", a=subject)

    collections = ["a", f001, "b", "39", "c", u"Persée", "c", u"Persée (adlr)"]
    marcrecord.add("980", subfields=collections)

    outputfile.write(marcrecord.as_marc())

inputfile.close()
outputfile.close()
