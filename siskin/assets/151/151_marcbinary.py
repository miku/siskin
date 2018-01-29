#/usr/bin/env python
# coding: utf-8

import io
import sys
import re

import html
import marcx


formatmap = {
    "Buch":
    {
        "leader": "cam",
        "007": "tu",
        "935b": "druck"
    },
    "DVD":
    {
        "leader": "ngm",
        "007": "vd",
        "935b": "dvdv",
        "935c": "vide"
    },
    "Blu-ray":
    {
        "leader": "ngm",
        "007": "vd",
        "935b": "bray",
        "935c": "vide"
    },
    "Videodatei":
    {
        "leader": "cam",
        "007": "cr",
        "935b": "cofz",
        "935c": "vide"
    },
    "CD":
    {
        "leader": "  m",
        "007": "c",
        "935b": "cdda"
    },
    "Videokassette":
    {
        "leader": "cgm",
        "007": "vf",
        "935b": "vika",
        "935c": "vide"
    },
    "Noten":
    {
        "leader": "nom",
        "007": "zm",
        "935c": "muno"
    },
    "Loseblattsammlung":
    {
        "leader": "nai",
        "007": "td",
    },
    "Film":
    {
        "leader": "cam",
        "007": "mu",
        "935b": "sobildtt"
    },
    "Aufsatz":
    {
        "leader": "naa",
        "007": "tu"
    },
}


def get_field(tag):
    regexp = re.search('<.*? tag="%s">(.*)$' % tag, field)
    if regexp:
        _field = regexp.group(1)
        _field = html.unescape(_field)
        return _field
    else:
        return ""

def get_subfield(tag, subfield):
    regexp = re.search('^<datafield.*tag="%s".*><subfield code="%s">(.*?)<\/subfield>' % (tag, subfield), field)
    if regexp:
        _field = regexp.group(1)
        _field = html.unescape(_field)
        return _field
    else:
        return ""

def get_leader(format="Buch"):
    return "     %s  22        4500" % formatmap[format]["leader"]

def get_field_007(format="Buch"):
    if "007" not in formatmap[format]:
        return ""
    return formatmap[format]["007"]

def get_field_935b(format="Buch"):
    if "935b" not in formatmap[format]:
        return ""
    return formatmap[format]["935b"]

def get_field_935c(format="Buch"):
    if "935c" not in formatmap[format]:
        return ""
    return formatmap[format]["935c"]


# Default input and output.
inputfilename = "151_input.xml"
outputfilename = "151_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = open(inputfilename, "r")    
outputfile = open(outputfilename, "wb")

records = inputfile.read()
records = records.split("</record>")

for record in records:

    format = ""
    f001 = ""
    f100a = ""
    f245a = ""
    f260a = ""
    f260b = ""
    f260c = ""
    f650a = ""
    f700a = ""

    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False
    fields = re.split("(</controlfield>|</datafield>)", record)
    
    subjects = []
    persons = []

    for field in fields:

        field = field.replace("\n", "")
        field = field.replace("</datafield>", "")
        field = field.replace("</controlfield>", "")
       
        # Identfikator
        if f001 == "":
            f001 = get_field("001")

        # Format
        if format == "":
            form = get_field("433")         
            
            regexp1 = re.search("\d\]?\sS\.", form)
            regexp2 = re.search("\d\]?\sSeit", form)
            regexp3 = re.search("\d\]?\sBl", form)
            regexp4 = re.search("\s?Illl?\.", form)
            regexp5 = re.search("[XVI],\s", form)
            regexp6 = re.search("^DVD", form)
            regexp7 = re.search("^Blu.?-ray", form)
            regexp8 = re.search("^H[DC] [Cc][Aa][Mm]", form)
            regexp9 = re.search("^HDCAM", form)
            regexp10 = re.search("[Bb]et.?-?[Cc]am", form)
            regexp11 = re.search("CD", form)
            regexp12 = re.search("[kKCc]asss?ette", form)
            regexp13 = re.search("^VHS", form)
            regexp14 = re.search("^Noten", form)
            regexp15 = re.search("^Losebl", form)
            regexp16 = re.search("^Film\s?\[", form)
            regexp17 = re.search("\d\smin", form)
            regexp18 = re.search("S\.\s\d+\s?-\s?\d+", form)

            if regexp1 or regexp2 or regexp3 or regexp4 or regexp5:
                format = "Buch"
            elif regexp6:
                format = "DVD"
            elif regexp7:
                format = "Blu-ray"
            elif regexp8 or regexp9 or regexp10:
                format = "Videodatei"
            elif regexp11:
                format = "CD"
            elif regexp12 or regexp13:
                format = "Videokassette"
            elif regexp14:
                format = "Noten"
            elif regexp15:
                format = "Loseblattsammlung"
            elif regexp16 or regexp17:
                format = "Film"
            elif regexp18:
                format = "Aufsatz"
            else:
                if form != "":
                    format = "Buch"
                    print("Format nicht erkannt: %s (Default = Buch)" % form)

        # 1. Urheber
        if f100a == "":
            f100a = get_subfield("100", "a")
        
        # Haupttitel
        if f245a == "":
            f245a = get_field("331")

        # Erscheinungsort
        if f260a == "":
            f260a = get_field("410")

        # Verlag
        if f260b == "":
            f260b = get_field("412")

        # Erscheinungsjahr
        if f260c == "":
            f260c = get_field("425")

        # Schlagwörter
        f650a = get_subfield("710", "a")
        if f650a != "":
            subjects.append(f650a)

        # weitere Personen
        # überprüfen, ob ein Personenfeld vorliegt, damit die Schleife
        # für die Personenfelder nicht bei jedem Feld durchlaufen wird
        regexp = re.search('tag="1\d\d"', field)
        if regexp:
           for i in range(101, 197):
               f700a = get_subfield(i, "a")
               if f700a != "":
                   persons.append(f700a)
                   break    
    
    if format == "":
        format = "Buch"

    leader = get_leader(format=format)
    marcrecord.leader = leader
    
    marcrecord.add("001", data="finc-151-" + f001)
    
    f007 = get_field_007(format=format)
    marcrecord.add("007", data=f007)
    
    marcrecord.add("100", a=f100a)
    
    marcrecord.add("245", a=f245a)
    
    publisher = ["a", "Hamburg : ", "b", f260b + ", ", "c", f260c]
    marcrecord.add("260", subfields=publisher)
    
    for subject in subjects:
        marcrecord.add("650", a=subject)
        
    for person in persons:
        marcrecord.add("700", a=person)

    f935b = get_field_935b(format=format)
    f935c = get_field_935c(format=format)
    marcrecord.add("935", b=f935b, c=f935c)

    collections = ["a", f001, "b", "151", "c", "Filmakademie Baden-Württemberg"]
    marcrecord.add("980", subfields=collections)

    outputfile.write(marcrecord.as_marc())

inputfile.close()
outputfile.close()