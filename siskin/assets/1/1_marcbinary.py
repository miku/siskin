#!/usr/bin/env python
# coding: utf-8

"""
Custom conversion for Gutenberg RDF, refs. #10875.

Usage:

    $ wget http://www.gutenberg.org/cache/epub/feeds/rdf-files.tar.zip
    $ unzip -p rdf-files.tar.zip | tar -xO | python 1_marcbinary.py -

"""

import base64
import fileinput
import io
import json
import re
import sys

import marcx
import xmltodict

inputfile = fileinput.input()

# Workaround, so we can stream to stdout as well.
outputfile = sys.stdout

if "-" not in sys.argv[1:]:
    outputfile = open("1_output.mrc", "wb")


langmap = {
    "da": "dan",
    "de": "ger",
    "el": "ell",
    "en": "eng",
    "eo": "epo",
    "es": "spa",
    "fi": "fin",
    "fr": "fre",
    "it": "ita",
    "la": "lat",
    "nl": "nld",
    "no": "nor",
    "pt": "por",
    "ro": "rom",
    "ru": "rus",
    "sv": "swe",
    "ja": "jpn",
    "he": "heb",
    "hu": "hun",
    "pl": "pol",
    "cy": "cym",
    "cs": "ces",
    "yi": "yid",
    "bg": "bul",
    "is": "isl",
    "ga": "gle",
    "sr": "srp",
    "af": "afr",
    "iu": "iku",
    "fy": "fry",
    "br": "bre",
    "arp": "arp",
    "lt": "lit",
    "tl": "tgl",
    "ca": "cat",
    "myn": "myn",
    "fur": "fur",
    "enm": "enm",
    "oc": "oci",
    "gla": "gla",
    "rmr": "rmr",
    "mi": "mri",
    "ilo": "ilo",
    "ia": "ina",
    "nap": "nap",
    "ceb": "ceb",
    "gl": "glg",
    "nah": "nah",
    "sl": "slv",
    "te": "tel",
    "oji": "oji",
    "ar": "ara",
    "et": "est",
    "fa": "fas",
    "sa": "san",
    "zh": "zho"
}

record = []
in_record = False
counter = 0

for line in inputfile:
    line = line.strip()

    f001 = ""
    f007 = "sr"
    f010a = ""
    f020a = ""
    f041a = ""
    f100a = ""
    f245a = ""
    f250a = ""
    f260b = ""
    f260c = ""
    f300 = ""
    f300a = ""
    f300b = ""
    f300c = ""
    f490a = ""
    f500a = ""
    f508a = ""
    f520a = ""
    f546a = ""
    f689a = ""
    f700a = ""
    f856u = ""
    files = []
    subjects = []
    authors = []
    leader_6_8 = "nam"

    if line.startswith("<rdf:RDF"):
        in_record = True

    if in_record:
        record.append(line)

    if line.startswith("</rdf:RDF>"):
        record = xmltodict.parse('\n'.join(record), force_list=("dcterms:hasFormat", "dcterms:subject", "dcterms:creator"))

        for file in record["rdf:RDF"]["pgterms:ebook"].get("dcterms:hasFormat", []):
            files.append(file.get("pgterms:file", {}).get("@rdf:about", ""))

        for file in files:

            if file.endswith(".mp3") or file.endswith(".ogg"):
                leader_6_8 = "cim"
                f007 = "su"

            regexp = re.search("(http:\/\/www\.gutenberg\.org\/ebooks\/\d+)", file)
            if regexp:
                f856u = regexp.group(1)
                break

        try:
            f001 = record["rdf:RDF"]["pgterms:ebook"]["@rdf:about"]
        except (TypeError, KeyError):
            pass

        try:
            f010a = record["rdf:RDF"]["pgterms:ebook"]["pgterms:marc010"]
        except (TypeError, KeyError):
            pass

        try:
            f020a = record["rdf:RDF"]["pgterms:ebook"]["pgterms:marc020"]
        except (TypeError, KeyError):
            pass

        try:
            f041a = record["rdf:RDF"]["pgterms:ebook"]["dcterms:language"]["rdf:Description"]["rdf:value"]["#text"]
        except (TypeError, KeyError):
            pass

        try:
            for creator in record["rdf:RDF"]["pgterms:ebook"]["dcterms:creator"]:
                author = creator["pgterms:agent"]["pgterms:name"]
                if author == "Various":
                    author = ""
                authors.append(author)
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
            f260b = record["rdf:RDF"]["pgterms:ebook"]["dcterms:publisher"]
        except (TypeError, KeyError):
            pass

        try:
            f260c = record["rdf:RDF"]["pgterms:ebook"]["dcterms:issued"]["#text"]
        except (TypeError, KeyError):
            pass

        try:
            f300 = record["rdf:RDF"]["pgterms:ebook"]["pgterms:marc300"]
        except (TypeError, KeyError):
            pass

        try:
            f490a = record["rdf:RDF"]["pgterms:ebook"]["pgterms:marc440"]
        except (TypeError, KeyError):
            pass

        try:
            f500a = record["rdf:RDF"]["pgterms:ebook"]["dcterms:description"]
        except (TypeError, KeyError):
            pass

        try:
            f508a = record["rdf:RDF"]["pgterms:ebook"]["pgterms:marc508"]
        except (TypeError, KeyError):
            pass

        try:
            f520a = record["rdf:RDF"]["pgterms:ebook"]["pgterms:marc520"]
        except (TypeError, KeyError):
            pass

        try:
            f546a = record["rdf:RDF"]["pgterms:ebook"]["pgterms:marc546"]
        except (TypeError, KeyError):
            pass

        for subject in record["rdf:RDF"]["pgterms:ebook"].get("dcterms:subject", []):
            subjects.append(subject.get("rdf:Description", {}).get("rdf:value", ""))

        marcrecord = marcx.Record(force_utf8=True)
        marcrecord.strict = False

        # Leader
        marcrecord.leader = "     %s  22        4500" % leader_6_8

        # 001
        regexp = re.search("ebooks\/(\d+)", f001)
        if regexp:
            f001 = regexp.group(1)
        else:
            pass
            #print("ID fehlt!")
        marcrecord.add("001", data="finc-1-%s" % f001)

        # 007
        marcrecord.add("007", data=f007)

        # Sprache für 008
        language = f041a
        f041a = langmap.get(f041a, "")
        if f041a == "":
            pass
            #print("Die Sprache %s fehlt in der Lookup-Tabelle!" % language)
        marcrecord.add("008", data="130227uu20uuuuuuxx uuup%s  c" % f041a)

        # Übergeordnetes Werk ???
        marcrecord.add("010", a=f010a)

        # ISBN
        marcrecord.add("020", a=f020a)

        # Sprache für 041a
        marcrecord.add("041", a=f041a)

        # 1. Urheber
        if len(authors) > 0:
            f100a = authors[0]
            marcrecord.add("100", a=f100a)

        # Titel
        marcrecord.add("245", a=f245a)

        # Ausgabevermerk ???
        marcrecord.add("250", a=f250a)

        # Erscheinungsjahr (bei Project Gutenberg)
        regexp = re.search("^(\d\d\d\d)", f260c)
        if regexp:
            f260c = regexp.group(1)
        else:
            f260c = ""

        # Erscheinungsvermerk
        marcrecord.add("260", b=f260b, c=f260c)

        # Kollationsangaben
        regexp = re.search("^\$a(.*?)\$b(.*?)\$c(.*?)$", f300)  # $a11 v. :$bill. ;$c24 cm
        if regexp:
            f300a, f300b, f300c = regexp.groups()
        else:
            f300a = f300
        marcrecord.add("300", a=f300a, b=f300b, c=f300c)

        # 440 ??? so in den Rohdaten
        marcrecord.add("490", a=f490a)

        # Fußnote (allgemein)
        marcrecord.add("500", a=f500a)

        # Fußnote (Credits)
        marcrecord.add("508", a=f508a)

        # Zusammenfassung
        marcrecord.add("520", a=f520a)
        marcrecord.add("546", a=f546a)

        # Schlagwörter
        for subject in subjects:
            marcrecord.add("689", a=subject)

        # 2. und mehr Urheber
        if len(authors) > 1:
            for f700a in authors[1:]:
                marcrecord.add("700", a=f700a)

        # URL
        marcrecord.add("856", q="text/html", _3="Link zur Ressource", u=f856u)

        # Medienform
        marcrecord.add("935", b="cofz", b="sott")

        # Ansigelung
        marcrecord.add("980", a=f001, b="1", c="Project Gutenberg")

        outputfile.write(marcrecord.as_marc())

        record = []
        in_record = False

    counter += 1

    # if counter == 500000:
    #    break

inputfile.close()

if outputfile == sys.stdout:
    outputfile.flush()
else:
    outputfile.close()
