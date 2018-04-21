#/usr/bin/env python
# coding: utf-8

import io
import sys
import re

from six.moves import html_parser
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

parser = html_parser.HTMLParser()


def get_field(tag):
    regexp = re.search('<.*? tag="%s">(.*)$' % tag, field)
    if regexp:
        _field = regexp.group(1)
        _field = parser.unescape(_field)
        return _field
    else:
        return ""


def get_subfield(tag, subfield):
    regexp = re.search('^<datafield.*tag="%s".*><subfield code="%s">(.*?)<\/subfield>' % (tag, subfield), field)
    if regexp:
        _field = regexp.group(1)
        _field = parser.unescape(_field)
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

inputfile = io.open(inputfilename, "r")
outputfile = io.open(outputfilename, "wb")

records = inputfile.read()
records = records.split("</record>")

for record in records:

    format = ""
    f001 = ""
    f020a = ""
    f100a = ""
    f100e = ""
    f245a = ""
    f260a = ""
    f260b = ""
    f260c = ""
    f300a = ""
    f650a = ""
    f700a = ""
    f700e = ""

    subjects = []
    persons = []
    f100 = []
    f700 = []

    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False
    fields = re.split("(</controlfield>|</datafield>)", record)

    for field in fields:

        field = field.replace("\n", "")
        field = field.replace("</datafield>", "")
        field = field.replace("</controlfield>", "")

        # Identfikator
        if f001 == "":
            f001 = get_field("001")

        # ISBN
        if f020a == "":
            f020a = get_subfield("540", "a")

        # Format
        if format == "":
            form = get_field("433")

            regexp1 = re.search("\d\]?\sS\.", form)
            regexp2 = re.search("\d\]?\sSeit", form)
            regexp3 = re.search("\d\]?\sBl", form)
            regexp4 = re.search("\s?Illl?\.", form)
            regexp5 = re.search("[XVI],\s", form)
            regexp6 = re.search("^\d+\s[SsPp]", form)
            regexp7 = re.search("^DVD", form)
            regexp8 = re.search("^Blu.?-[Rr]ay", form)
            regexp9 = re.search("^H[DC] [Cc][Aa][Mm]", form)
            regexp10 = re.search("^HDCAM", form)
            regexp11 = re.search("[Bb]et.?-?[Cc]am", form)
            regexp12 = re.search("CD", form)
            regexp13 = re.search("[kKCc]asss?ette", form)
            regexp14 = re.search("^VHS", form)
            regexp15 = re.search("^Noten", form)
            regexp16 = re.search("^Losebl", form)
            regexp17 = re.search("^Film\s?\[", form)
            regexp18 = re.search("\d\smin", form)
            regexp19 = re.search("S\.\s\d+\s?-\s?\d+", form)

            if regexp1 or regexp2 or regexp3 or regexp4 or regexp5 or regexp6:
                format = "Buch"
            elif regexp7:
                format = "DVD"
            elif regexp8:
                format = "Blu-ray"
            elif regexp9 or regexp10 or regexp11:
                format = "Videodatei"
            elif regexp12:
                format = "CD"
            elif regexp13 or regexp14:
                format = "Videokassette"
            elif regexp15:
                format = "Noten"
            elif regexp16:
                format = "Loseblattsammlung"
            elif regexp17 or regexp18:
                format = "Film"
            elif regexp19:
                format = "Aufsatz"
            else:
                if form != "":
                    format = "Buch"
                    print("Format nicht erkannt: %s (Default = Buch)" % form)

        # 1. Urheber
        if f100a == "":
            f100a = get_subfield("100", "a")
            if f100a != "":
                f100.append("a")
                f100.append(f100a)
                f100e = get_subfield("100", "b")
                if f100e != "":
                    f100e = f100e.split("]")
                    for role in f100e[:-1]:  # Slice :-1 damit das letzte ] ausgelassen wird
                        role = role + "]"
                        f100.append("e")
                        f100.append(role)

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

        # physische Beschreibung
        if f300a == "":
            f300 = get_field("433")
            # 335 S. : zahlr. Ill. ; 32 cm
            regexp1 = re.search("(.*)\s?:\s(.*);\s(.*)", f300)
            # 289 S.: Zahlr. Ill.
            regexp2 = re.search("(.*)\s?:\s(.*)", f300)
            # 106 S. ; 21 cm
            regexp3 = re.search("(.*)\s?;\s(.*)", f300)

            if regexp1:
                f300a, f300b, f300c = regexp1.groups()
                f300a = f300a + " : "
                f300b = f300b + " ; "
            elif regexp2:
                f300a, f300b = regexp2.groups()
                f300a = f300a + " : "
                f300c = ""
            elif regexp3:
                f300a, f300c = regexp3.groups()
                f300a + " ; "
                f300b = ""
            else:
                f300a = f300
                f300b = ""
                f300c = ""

        # Schlagwörter
        f650a = get_subfield("710", "a")
        if f650a != "":
            subjects.append(f650a)

        # weitere Personen
        regexp = re.search('tag="1\d\d"', field)
        if regexp:
            for i in range(101, 197):  # überprüfen, ob ein Personenfeld vorliegt, damit die Schleife für die Personenfelder nicht bei jedem Feld durchlaufen wird
                f700a = get_subfield(i, "a")
                if f700a != "":
                    f700.append("a")
                    f700.append(f700a)
                    f700e = get_subfield(i, "b")
                    if f700e != "":
                        f700e = f700e.split("]")
                        for role in f700e[:-1]:  # Slice :-1 damit das letzte ] ausgelassen wird
                            role = role + "]"
                            f700.append("e")
                            f700.append(role)
                    persons.append(f700)
                    f700 = []
                    break

    if format == "":
        format = "Buch"

    leader = get_leader(format=format)
    marcrecord.leader = leader

    marcrecord.add("001", data="finc-151-" + f001)

    f007 = get_field_007(format=format)
    marcrecord.add("007", data=f007)

    marcrecord.add("020", a=f020a)

    marcrecord.add("100", subfields=f100)

    marcrecord.add("245", a=f245a)

    publisher = ["a", "Hamburg : ", "b", f260b + ", ", "c", f260c]
    marcrecord.add("260", subfields=publisher)

    physicaldescription = ["a", f300a, "b", f300b, "c", f300c]
    marcrecord.add("300", subfields=physicaldescription)

    for f650a in subjects:
        marcrecord.add("650", a=f650a)

    for f700 in persons:
        marcrecord.add("700", subfields=f700)

    f935b = get_field_935b(format=format)
    f935c = get_field_935c(format=format)
    marcrecord.add("935", a="vkfilm", b=f935b, c=f935c)

    collections = ["a", f001, "b", "151", "c", u"Filmakademie Baden-Württemberg"]
    marcrecord.add("980", subfields=collections)

    outputfile.write(marcrecord.as_marc())

inputfile.close()
outputfile.close()
