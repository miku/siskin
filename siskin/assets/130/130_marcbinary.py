#!/usr/bin/env python3
# coding: utf-8

import io
import sys
import re

import tqdm
import marcx
import collections

formatmap = {
    "p||||||z|||||||": "Zeitschrift",
    "z||||||z|||||||": "Zeitung",
    "r||||||z|||||||": "Reihe",
    "r||||||||||||||": "Reihe",
    "a|a|||||||||||": "Monografie",
    "a|||||||||||||": "Monografie",
    "a|a|||||||": "Monografie",
    "|||||ca||||||": "Videokassette",
    "a|||||||g|||||": "Datenträger",
    "||||||||g|": "Datenträger"
}

def get_field(tag):
    regexp = re.search('<.*?\snr="%s".*>(.*)$' % tag, field)
    if regexp:
        _field = regexp.group(1)
        return _field
    else:
        return ""

def get_subfield(tag, subfield):
    regexp = re.search('<feld.*nr="%s".*><uf code="%s">(.*?)<\/uf>' % (tag, subfield), field)
    if regexp:
        _field = regexp.group(1)
        return _field
    else:
        return ""

inputfilename = "130_input.xml"
outputfilename = "130_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

hierarchymap = collections.defaultdict(list)

with io.open(inputfilename, "r") as inputfile:

    records = inputfile.read()
    records = records.split("</datensatz>")

    for record in records:

        f010 = ""
        f089 = ""

        record = record.replace("\n", "")
        pattern = """<feld nr="010" ind=" ">(.*?)</feld>.*?nr="089" ind=" ">(.*?)</feld>"""
        regexp = re.search(pattern, record)
        if regexp:
            f010, f089 = regexp.groups()

        if f010 != "" and f089 != "":
            f010 = f010.lstrip("0")
            hierarchymap[f010].append(f089)

    #print(hierarchymap)

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = io.open(inputfilename, "r")
outputfile = io.open(outputfilename, "wb")

records = inputfile.read()
records = records.split("</datensatz>")

for record in records:

    f001 = ""
    f007 = ""
    f020a = ""
    f041a = ""
    f100a = ""
    f100e = ""
    f110a = ""
    f245a = ""
    f245b = ""
    f245c = ""
    f250a = ""
    f260a = ""
    f260b = ""
    f260c = ""
    f300a = ""
    f300b = ""
    f300c = ""
    f500a = ""
    f650a = ""
    f700e = ""
    f856u = ""
    f866a = ""
    format1 = ""
    format2 = ""
    subjects = []
    persons = []
    corporates = []

    marcrecord = marcx.Record(force_utf8=True, to_unicode=True)
    marcrecord.strict = False
    fields = re.split("(</feld>)", record)

    for field in fields:

        field = field.replace("\n", "")
        field = field.replace("</feld>", "")
        field = field.replace("      ", "")

        # Identfikator
        if f001 == "":
            f001 = get_field("001")

        # ISBN
        if f020a == "":
            f020a = get_field("540")
            if f020a != "":
                regexp = re.search("\s?([\d-]+)\s?", f020a)
                if regexp:
                    f020a = regexp.group(1)
                else:
                    print("Die ISBN konnte nicht bereinigt werden: " + f020a)

        # Sprache
        if f041a == "":
            f041a = get_field("037")

        # 1. Urheber
        if f100a == "":
            f100a = get_field("100")
            if f100a != "":
                regexp = re.search(".\s¬(\[.*\])", f100a)
                if regexp:
                    f100e = regexp.group(1)
                    f100a = re.sub(".\s¬\[.*\]¬", "", f100a)

        # 1. Körperschaft
        if f110a == "":
            f110a = get_field("200")

        # Haupttitel
        if f245a == "":
            f245a = get_field("331")

        # Titelzusatz
        if f245b == "":
            f245b = get_field("335")

        # Verantwortlichenangabe
        if f245c == "":
            f245c = get_field("359")

        # Ausgabe
        if f250a == "":
            f250a = get_field("403")

        # Erscheinungsort
        if f260a == "":
            f260a = get_field("410")
            if f260a == "":
                f260a = get_subfield("419", "a")

        # Verlag
        if f260b == "":
            f260b = get_field("412")
            if f260b == "":
                f260b = get_subfield("419", "b")
            if f260b != "":
                f260b = " : " + f260b

        # Erscheinungsjahr
        if f260c == "":
            f260c = get_field("425")
            if f260c == "":
                f260c = get_subfield("419", "c")
            if f260c != "" and (f260a != "" or f260b != ""):
                f260c = ", " + f260c

        # Seitenzahl
        if f300a == "":
            f300a = get_field("433")

        # Illustrationen
        if f300b == "":
            f300b = get_field("434")
            if f300b != "":
                f300b = " : " + f300b

        # Format
        if f300c == "":
            f300c = get_field("435")
            if f300c != "":
                f300c = " ; " + f300c

        # Schlagwörter
        if f650a == "":
            f650a = get_field("710")
            if f650a != "":
                subjects.append(f650a)
                f650a = ""

        # weitere Personen
        regexp = re.search('nr="1\d\d"', field)
        if regexp:
            for i in range(101, 197):  # überprüfen, ob ein Personenfeld vorliegt, damit die Schleife für die Personenfelder nicht bei jedem Feld durchlaufen wird
                f700a = get_field(i)
                if f700a != "":
                    regexp = re.search("^\d+", f700a) # die Felder, die nur Personen-IDs enthalten, werden übersprungen
                    if not regexp:
                        regexp = re.search(".\s¬(\[.*\])", f700a)
                        if regexp:
                            f700e = regexp.group(1)
                            f700a = re.sub(".\s¬\[.*\]¬", "", f700a)
                        persons.append(f700a)
                        break

        # weitere Körperschaften
        regexp = re.search('nr="2\d\d"', field)
        if regexp:
            for i in range(201, 299):
                f710a = get_field(i)
                if f710a != "":
                    regexp = re.search("^\d+", f710a)
                    if not regexp:
                        corporates.append(f710a)
                        break

        # Link zum Datensatz
        if f856u == "":
            f856u = get_subfield("655", "u")

        #Bestandsnachweis (866a)
        f866a = f001.lstrip("0")
        f866a = hierarchymap.get(f866a, "")

        if format1 == "":
            format1 = get_field("052")

        if format2 == "":
            format2 = get_field("050")

    if format1 != "" or format2 != "":
        format = format1 or format2
    else:
        format = "a|a|||||||||||"

    if formatmap.get(format) == "Zeitschrift":
        leader = "     cas a2202221   4500"
        f007 = "tu"
        f008 = "                     p"
        f935b = "druck"
        f935c = "az"
    elif formatmap.get(format) == "Zeitung":
        leader = "     cas a2202221   4500"
        f007 = "tu"
        f008 = "                     n"
        f935b = "druck"
        f935c = "az"
    elif formatmap.get(format) == "Reihe":
        leader = "     cas a2202221   4500"
        f007 = "tu"
        f008 = "                     m"
        f935b = "druck"
        f935c = "az"
    elif formatmap.get(format) == "Monografie":
        leader = "     nam  22        4500"
        f007 = "tu"
        f008 = ""
        f935b = "druck"
        f935c = ""
    elif formatmap.get(format) == "Videokassette":
        leader = "     cgm  22        4500"
        f007 = "v"
        f008 = ""
        f935b = "vika"
        f935c = "vide"
    elif formatmap.get(format) == "Datenträger":
        leader = "     cgm  22        4500"
        f007 = "v"
        f008 = ""
        f935b = "soerd"
        f935c = ""
    else:
        print("Format %s ist nicht in der Mapping-Tabelle enthalten" % format)
        leader = "     nam  22        4500"
        f007 = "tu"
        f008 = ""
        f935b = "druck"
        f935c = ""

    if f245a == "" or "Arkady" in f245a: # einzelne Zeitschriftenhefte und die fehlerhaften Arkady-Records werden übersprungen
        continue

    assert(len(leader) == 24)
    marcrecord.leader = leader
    marcrecord.add("001", data="finc-130-" + f001)
    marcrecord.add("007", data=f007)
    marcrecord.add("008", data=f008)
    marcrecord.add("020", a=f020a)
    marcrecord.add("041", a=f041a)
    marcrecord.add("100", a=f100a, e=f100e)
    marcrecord.add("110", a=f110a)
    titleparts = ["a", f245a, "b", f245b, "c", f245c]
    marcrecord.add("245", subfields=titleparts)
    marcrecord.add("250", a=f250a)
    publisher = ["a", f260a, "b", f260b, "c", f260c]
    marcrecord.add("260", subfields=publisher)
    physicaldescription = ["a", f300a, "b", f300b, "c", f300c]
    marcrecord.add("300", subfields=physicaldescription)
    for subject in subjects:
        marcrecord.add("650", a=subject)
    for person in persons:
        marcrecord.add("700", a=person, e=f700e)
    for corporate in corporates:
        marcrecord.add("710", a=corporate)
    if f856u != "":
        marcrecord.add("856", q="text/html", u=f856u)
    f866a = "; ".join(f866a)
    marcrecord.add("866", a=f866a)
    marcrecord.add("935", b=f935b, c=f935c)
    marcrecord.add("980", a=f001, b="130", c="VDEH")

    outputfile.write(marcrecord.as_marc())

inputfile.close()
outputfile.close()
