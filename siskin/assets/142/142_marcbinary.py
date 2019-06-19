#!/usr/bin/env python3
# coding: utf-8

# SID: 142
# Ticket: #8392
# TechnicalCollectionID: sid-142-col-gesamtkatduesseldorf
# Task:

import io
import re
import sys

import xmltodict

import marcx
from siskin.mab import MabXMLFile
from siskin.utils import marc_build_imprint

inputfilename = "142_input.xml"
outputfilename = "142_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

reader = MabXMLFile(inputfilename, replace=((u"¬", ""), ))
outputfile = open(outputfilename, "wb")

parent_ids = []
parent_titles = {}

for record in reader:
   
    parent_id = record.field("010", alt="")

    if len(parent_id) > 0:
        parent_ids.append(parent_id)

for record in reader:

    id = record.field("001")
    title = record.field("331")

    if id in parent_ids:
        parent_titles[id] = title

for record in reader:

    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False

    parent_id = record.field("010", alt="")
    id = record.field("001")
    title = record.field("331")

    f245a = title
    f245p = ""
    f773w = ""

    if len(parent_id) > 0:
        has_parent_title = parent_titles.get(parent_id, None)
        if has_parent_title:
            f245a = parent_titles[parent_id]
            f245p = title
            f773w = "(DE-576)" + parent_id

    # Format
    format = record.field("433", alt="")
    regexp1 = re.search("\d\.?\sS", format)

    if id in parent_ids:
        leader = "     cam  22       a4500"
        f935b = ""
        f935c = ""
    elif "Seiten" in format or "Blatt" in format or "nicht gez" in format or format == "" or "Zählung" in format or regexp1:
        leader = "     cam  22        4500"
        f935b = "druck"
        f935c = "lo"
    elif "Faltbl" in format:
        leader = "     cam  22        4500"
        f935b = "plakat"
        f935c = ""
    elif "CD-ROM" in format:
        leader = "     cam  22        4500"
        f935b = "crom"
        f935c = ""
    elif "DVD-ROM" in format:
        leader = "     cam  22        4500"
        f935b = "dvdv"
        f935c = ""
    elif "Compact Disc" in format:
        leader = "     cam  22        4500"
        f935b = "cdda"
        f935c = ""
    else:
        print("050 ", record.field("050"))
        print("433 ", record.field("433"))
        print("540 ", record.field("540"))
        print("\n")
        #break

    # Leader
    marcrecord.leader = leader

    # Identifier
    f001 = record.field("001")
    f001 = "finc-142-" + f001
    marcrecord.add("001", data=f001)

    # 007
    marcrecord.add("007", data="tu")

    # ISBN
    isbns = record.fields("540")
    for f020a in isbns:
        f020a = f020a.replace("ISBN ", "")
        marcrecord.add("020", a=f020a)

    # ISSN
    issns = record.fields("542")
    for f022a in issns:
        f022a = f022a.replace("ISSN ", "")
        marcrecord.add("022", a=f022a)

    # Sprache
    f041a = record.field("037")
    if f041a:
        marcrecord.add("041", a=f041a)

    # 1. Schöpfer
    f100a = record.field("100")
    marcrecord.add("100", a=f100a)

    # 1. Körperschaft
    f110a = record.field("200")
    marcrecord.add("110", a=f110a)

    # Haupttitel & Verantwortlichenangabe
    f245b = record.field("335")
    f245c = record.field("359")
    f245 = ["a", f245a, "b", f245b, "c", f245c, "p", f245p]
    marcrecord.add("245", subfields=f245)

    # Erscheinungsvermerk
    f260a = record.field("410", alt="")
    f260b = record.field("412", alt="")
    f260c = record.field("425", alt="")
    subfields = marc_build_imprint(f260a, f260b, f260c)
    marcrecord.add("260", subfields=subfields)

    # Umfangsangabe
    f300a = record.field("433")
    f300b = record.field("434")
    subfields = ["a", f300a, "b", f300b]
    marcrecord.add("300", subfields=subfields)

    # Reihe
    f490a = record.field("451")
    marcrecord.add("490", a=f490a)
    f490a = record.field("461")
    marcrecord.add("490", a=f490a)

    # weitere geistige Schöpfer
    for i in range(104, 199, 4):
        tag = str(i)
        f700a = record.field(tag)
        marcrecord.add("700", a=f700a)

    # weitere Körperschaften
    for i in range(204, 299, 4):
        tag = str(i)
        f710a = record.field(tag)
        marcrecord.add("710", a=f710a)

    # übergeordnetes Werk
    marcrecord.add("773", w=f773w)

    # Link zu Datensatz und Ressource
    f655z = record.field("655", "z")
    f6553 = record.field("655", "3")
    f856u = record.field("655", "u")
    if f655z:
        f856q = f655z
    else:
        f856q = f6553
    marcrecord.add("856", q=f856q, u=f856u)

    # Kollektion
    marcrecord.add("912", a="vkfilm")

    # Medientyp
    marcrecord.add("935", b=f935b, c=f935c)

    # Ansigelung
    f001 = record.field("001")
    collections = ["a", f001, "b", "142", "c", "sid-142-col-gesamtkatduesseldorf"]
    marcrecord.add("980", subfields=collections)

    outputfile.write(marcrecord.as_marc())

outputfile.close()
