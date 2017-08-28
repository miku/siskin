#!/usr/bin/env python3
# coding: utf-8

import io
import re
import sys
import json
import marcx
import base64
import xmltodict
import fileinput

inputfile = fileinput.input()
outputfile = open("1_output.mrc", "wb")

record = []
in_record = False
counter = 0

for line in inputfile:
    line = line.strip()

    f001 = ""
    f010a = ""
    f020a = ""
    f041a = ""
    f100a = ""
    f245a = ""
    f250a = ""
    f260a = ""
    f300a = ""
    f300b = ""
    f300c = ""
    f440a = ""
    f500a = ""
    f508a = ""
    f520a = ""
    f546a = ""
    f689a = ""
    f856u = ""
    files = []
    subjects = []

    if line.startswith("<rdf:RDF"):
        in_record = True

    if in_record:
        record.append(line)

    if line.startswith("</rdf:RDF>"):
        record = xmltodict.parse('\n'.join(record), force_list=("dcterms:hasFormat", "dcterms:subject"))

        for file in record["rdf:RDF"]["pgterms:ebook"].get("dcterms:hasFormat", []):
            files.append(file.get("pgterms:file", {}).get("@rdf:about", ""))

        for file in files:
            if file.endswith('.txt'):
                f856u = file
                f001 = file
                f001 = f001.encode("utf-8")
                f001 = base64.b64encode(f001)
                f001 = f001.decode("ascii")
                f001 = f001.rstrip("=")

        try:
            f010a = record["rdf:RDF"]["pgterms:ebook"]["dcterms:marc010"]["#text"]
        except (TypeError, KeyError):
            pass

        try:
            f020a = record["rdf:RDF"]["pgterms:ebook"]["dcterms:marc020"]["#text"]
        except (TypeError, KeyError):
            pass

        try:
            f041a = record["rdf:RDF"]["pgterms:ebook"]["dcterms:language"]["#text"]
        except (TypeError, KeyError):
           pass

        try:
            f100a = record["rdf:RDF"]["pgterms:ebook"]["dcterms:creator"]["pgterms:agent"]["pgterms:name"]
            if f100a == "Various":
                f100a = ""
        except (TypeError, KeyError):
            pass

        try:
            f245a = record["rdf:RDF"]["pgterms:ebook"]["dcterms:title"]
        except (TypeError, KeyError):
            pass

        try:
            f250a = record["rdf:RDF"]["pgterms:ebook"]["pgterms:marc250"]
        except (TypeError, KeyError):
            pass

        try:
            f260a = record["rdf:RDF"]["pgterms:ebook"]["dcterms:publisher"]
        except (TypeError, KeyError):
            pass

        try:
            value = record["rdf:RDF"]["pgterms:ebook"]["dcterms:marc300"]
            regexp = re.search("^\$a(.*?)\$b(.*?)\$c(.*?)$", value) #$a11 v. :$bill. ;$c24 cm
            if regexp:
                f300a, f300b, f300c = regexp.groups()
            else:
                f300a = value
        except (TypeError, KeyError):
            pass

        try:
            f440a = record["rdf:RDF"]["pgterms:ebook"]["dcterms:marc440"]
        except (TypeError, KeyError):
            pass

        try:
            f500a = record["rdf:RDF"]["pgterms:ebook"]["dcterms:description"]
        except (TypeError, KeyError):
            pass

        try:
            f508a = record["rdf:RDF"]["pgterms:ebook"]["dcterms:marc508"]
        except (TypeError, KeyError):
            pass

        try:
            f520a = record["rdf:RDF"]["pgterms:ebook"]["dcterms:marc520"]
        except (TypeError, KeyError):
            pass

        try:
            f546a = record["rdf:RDF"]["pgterms:ebook"]["dcterms:marc546"]
        except (TypeError, KeyError):
            pass

        for subject in record["rdf:RDF"]["pgterms:ebook"].get("dcterms:subject", []):
            subjects.append(subject.get("rdf:Description", {}).get("rdf:value", ""))

        marcrecord = marcx.Record(force_utf8=True)
        marcrecord.strict = False
        marcrecord.leader = "     ncs  22        450 "
        marcrecord.add("001", data="finc-1-%s" % f001)
        marcrecord.add("007", data="cr")
        marcrecord.add("008", data="130227uu20uuuuuuxx uuup%s  c" % f041a)
        marcrecord.add("010", a=f010a)
        marcrecord.add("020", a=f020a)
        marcrecord.add("041", a=f041a)
        marcrecord.add("100", a=f100a)
        marcrecord.add("245", a=f245a)
        marcrecord.add("250", a=f250a)
        marcrecord.add("260", a=f260a)
        marcrecord.add("300", a=f300a, b=f300b, c=f300c)
        marcrecord.add("440", a=f440a)
        marcrecord.add("500", a=f500a)
        marcrecord.add("508", a=f508a)
        marcrecord.add("520", a=f520a)
        marcrecord.add("546", a=f546a)
        for subject in subjects:
            marcrecord.add("689", a=subject)
        marcrecord.add("856", q="text/html", _3="Link zur Ressource", u=f856u)
        marcrecord.add("980", a=f001, b="1", c="Project Gutenberg")
        outputfile.write(marcrecord.as_marc())

        record = []
        in_record = False

    counter += 1

    #if counter == 500000:
    #    break

inputfile.close()
outputfile.close()