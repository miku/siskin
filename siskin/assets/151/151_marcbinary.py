#/usr/bin/env python
# coding: utf-8

import io
import sys
import re

import marcx


def get_field(tag):

    regexp = re.search('<.*? tag="%s">(.*)$' % tag, field)

    if regexp:
        return regexp.group(1)
    else:
        return ""

def get_subfield(tag, subfield):

    regexp = re.search('^<datafield.*tag="%s".*><subfield code="%s">(.*?)<\/subfield>' % (tag, subfield), field)

    if regexp:
        return regexp.group(1)
    else:
        return ""


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

    f001 = ""
    f100a = ""
    f245a = ""
    f260a = ""
    f260b = ""
    f260c = ""
    f650a = ""
    f700a = ""

    marcrecord = marcx.Record(force_utf8=True)
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


    marcrecord.leader= "     nam  22        4500"
    marcrecord.add("001", data="finc-151-" + f001)
    marcrecord.add("005", data="tu")
    marcrecord.strict = False
    marcrecord.add("100", a=f100a)
    marcrecord.add("245", a=f245a)
    publisher = ["a", "Hamburg : ", "b", f260b + ", ", "c", f260c]
    marcrecord.add("260", subfields=publisher)
    
    for subject in subjects:
        marcrecord.add("650", a=subject)
        
    for person in persons:
        marcrecord.add("700", a=person)

    collections = ["a", f001, "b", "151", "c", "Filmakademie Baden-Württemberg"]
    marcrecord.add("980", subfields=collections)

    outputfile.write(marcrecord.as_marc())

inputfile.close()
outputfile.close()