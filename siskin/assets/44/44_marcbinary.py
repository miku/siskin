#!/usr/bin/env python3
# coding: utf-8

# Quelle: Deutsches Textarchiv
# SID: 44
# Tickets: #14972
# technicalCollectionID: sid-44-col-textarchiv
# Task: TODO

import base64
import io
import os
import re
import sys

import xmltodict

import marcx
from siskin.utils import marc_build_imprint

inputfilename = "44_input.xml"
outputfilename = "44_output.mrc"

if len(sys.argv) >= 3:
    inputfilename, outputfilename = sys.argv[1:3]

inputfile = io.open(inputfilename, "rb")
outputfile = io.open(outputfilename, "wb")

inputfile = open(inputfilename, "rb")
xmlfile = inputfile.read()
records = xmltodict.parse(xmlfile,
                          force_list=("cmdp:title", "cmdp:author", "cmdp:editor", "cmdp:pubPlace", "cmdp:publisher",
                                      "cmdp:date"))
outputfile = open(outputfilename, "wb")

for xmlrecord in records["Records"]["Record"]:

    if not xmlrecord["metadata"]["cmd:CMD"]["cmd:Components"]["cmdp:teiHeader"]["cmdp:fileDesc"].get("cmdp:titleStmt"):
        continue

    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False

    # Leader
    marcrecord.leader = "     nam  22        4500"

    # Identifikator
    id = xmlrecord["header"]["identifier"]
    id = bytes(id)
    id = base64.b64encode(id)
    f001 = id.decode("utf-8").rstrip("=")
    marcrecord.add("001", data="finc-44-" + f001)

    # Zugangsformat
    marcrecord.add("007", data="cr")

    # Sortierjahr
    imprint = xmlrecord["metadata"]["cmd:CMD"]["cmd:Components"]["cmdp:teiHeader"]["cmdp:fileDesc"]["cmdp:sourceDesc"][
        "cmdp:biblFull"].get("cmdp:publicationStmt", "")
    if imprint:
        year = imprint["cmdp:date"][0]["#text"]
        match = re.match("\d\d\d\d", year)
        if match:
            marcrecord.add("008", data="0000000%s" % year)

    # Sprache
    marcrecord.add("041", a="ger")

    # 1. Urheber
    authors = xmlrecord["metadata"]["cmd:CMD"]["cmd:Components"]["cmdp:teiHeader"]["cmdp:fileDesc"][
        "cmdp:titleStmt"].get("cmdp:author", "")
    if authors:
        surname = authors[0]["cmdp:persName"].get("cmdp:surname", "")
        forename = authors[0]["cmdp:persName"].get("cmdp:forename", "")
        rolename = authors[0]["cmdp:persName"].get("cmdp:roleName", "")
        if rolename and forename:
            rolename = " <" + rolename + ">"
            f100a = forename + rolename
        elif surname and forename:
            f100a = surname + ", " + forename
        elif surname and surname != "N. N.":
            f100a = surname
        else:
            f100a = ""
        marcrecord.add("100", a=f100a, e="verfasserin", _4="aut")

    # Titlel
    titles = xmlrecord["metadata"]["cmd:CMD"]["cmd:Components"]["cmdp:teiHeader"]["cmdp:fileDesc"]["cmdp:titleStmt"][
        "cmdp:title"]
    f245a = titles[0]["#text"]
    f245b = ""
    f245n = ""
    if len(titles) == 2:
        for title in titles:
            if "#text" in title:
                f245b = title["#text"]
            if "@n" in title:
                f245n = title["@n"]
    marcrecord.add("245", a=f245a, b=f245b, n=f245n)

    # Ausgabe
    edition = xmlrecord["metadata"]["cmd:CMD"]["cmd:Components"]["cmdp:teiHeader"]["cmdp:fileDesc"].get(
        "cmdp:editionStmt", "")
    if edition:
        f250a = edition["cmdp:edition"]
        marcrecord.add("250", a=f250a)

    # Erscheinungsvermerk
    imprint = xmlrecord["metadata"]["cmd:CMD"]["cmd:Components"]["cmdp:teiHeader"]["cmdp:fileDesc"]["cmdp:sourceDesc"][
        "cmdp:biblFull"].get("cmdp:publicationStmt", "")
    if imprint:
        f260a = ""
        f260b = ""
        f260c = ""
        place = imprint.get("cmdp:pubPlace")
        if place:
            f260a = place[0]
        publisher = imprint.get("cmdp:publisher")
        if publisher:
            f260b = publisher[0]["cmdp:name"]
            if not f260b:  # sometimes exists but None
                f260b = ""
        f260c = imprint["cmdp:date"][0]["#text"]
        subfields = marc_build_imprint(f260a, f260b, f260c)
        marcrecord.add("260", subfields=subfields)

    # Umfangsangabe
    pages = xmlrecord["metadata"]["cmd:CMD"]["cmd:Components"]["cmdp:teiHeader"]["cmdp:fileDesc"]["cmdp:sourceDesc"][
        "cmdp:biblFull"].get("cmdp:extent", "")
    if pages:
        f300a = pages["cmdp:measure"]["#text"]
        marcrecord.add("300", a=f300a)

    # Inhaltsbeschreibung
    abstract = xmlrecord["metadata"]["cmd:CMD"]["cmd:Components"]["cmdp:teiHeader"]["cmdp:profileDesc"].get(
        "cmdp:abstract", "")
    if abstract:
        f520a = abstract["cmdp:p"]
        marcrecord.add("520", a=f520a)

    # Schlagwörter
    subjects = xmlrecord["metadata"]["cmd:CMD"]["cmd:Components"]["cmdp:teiHeader"]["cmdp:profileDesc"].get(
        "cmdp:textClass", "")
    if subjects:
        for subject in subjects["cmdp:classCode"]:
            subject = subject["#text"]
            if subject[0].isupper():
                subjects = re.split("[;,]", subject)
                for subject in subjects:
                    f650a = subject.strip()
                    marcrecord.add("650", a=f650a)

    # Weitere Urheber
    authors = xmlrecord["metadata"]["cmd:CMD"]["cmd:Components"]["cmdp:teiHeader"]["cmdp:fileDesc"][
        "cmdp:titleStmt"].get("cmdp:author", "")
    if authors:
        for author in authors[1:]:
            surname = author["cmdp:persName"].get("cmdp:surname", "")
            forename = author["cmdp:persName"].get("cmdp:forename", "")
            rolename = author["cmdp:persName"].get("cmdp:roleName", "")
            if rolename and forename:
                rolename = " <" + rolename + ">"
                f700a = forename + rolename
            elif surname and forename:
                f700a = surname + ", " + forename
            elif surname and surname != "N. N.":
                f700a = surname
            else:
                f700a = ""
            marcrecord.add("700", a=f700a, e="verfasserin", _4="aut")

    # Herausgeber
    editors = xmlrecord["metadata"]["cmd:CMD"]["cmd:Components"]["cmdp:teiHeader"]["cmdp:fileDesc"][
        "cmdp:titleStmt"].get("cmdp:editor", "")
    if authors:
        for author in authors[1:]:
            surname = author["cmdp:persName"].get("cmdp:surname", "")
            forename = author["cmdp:persName"].get("cmdp:forename", "")
            if surname and forename:
                f700a = surname + ", " + forename
            elif surname and surname != "N. N.":
                f700a = surname
            else:
                f700a = ""
            marcrecord.add("700", a=f700a, e="hrsg", _4="edt")

    # Link zur Ressource
    urls = xmlrecord["metadata"]["cmd:CMD"]["cmd:Resources"]["cmd:ResourceProxyList"]["cmd:ResourceProxy"]
    xml = urls[0]["cmd:ResourceRef"]
    html = urls[1]["cmd:ResourceRef"]
    txt = urls[2]["cmd:ResourceRef"]
    page = urls[3]["cmd:ResourceRef"]
    marcrecord.add("856", q="text/xml", _3="Download als XML", u=xml)
    marcrecord.add("856", q="text/html", _3="Download als HTML", u=html)
    marcrecord.add("856", q="text/plain", _3="Download als Text", u=txt)
    marcrecord.add("856", q="text/html", _3="Link zur Seite", u=page)

    # Format
    marcrecord.add("935", b="cofz")

    # Kollektion
    collections = ["a", f001, "b", "44", "c", "sid-44-col-textarchiv"]
    marcrecord.add("980", subfields=collections)

    outputfile.write(marcrecord.as_marc())

outputfile.close()
