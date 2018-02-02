#!/usr/bin/env python3
# coding: utf-8

import io
import sys
import re

import marcx


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

inputfile = open(inputfilename, "r")    
outputfile = open(outputfilename, "wb")

records = inputfile.read()
records = records.split("</datensatz>")

for record in records:

    f001 = ""
    f007 = ""
    f020a = ""
    f041a = ""
    f100a = ""
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
    f650a = ""
    subjects = []
    persons = []

    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False
    fields = re.split("(</feld>)", record)
  
    for field in fields:

        field = field.replace("\n", "")
        field = field.replace("</feld>", "")
        field = field.replace("      ", "")

        # Identfikator
        if f001 == "":
            f001 = get_field("001")

        # Format
        if f007 == "":
            f007 = get_field("050")
            # ToDo: Format-Mapping MAB -> MARC

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

        # Titel
        if f245a == "":
            f245a = get_field("331")

        # Titel
        if f245b == "":
            f245b = get_field("335")

        # Titel
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
                        persons.append(f700a)
                        break


    marcrecord.leader = "     cam  22        4500"
    marcrecord.add("001", data="finc-130-" + f001)
    marcrecord.add("007", data="tu")
    marcrecord.add("020", a=f020a)
    marcrecord.add("041", a=f041a)
    marcrecord.add("100", a=f100a)    
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
        marcrecord.add("700", a=person)
    marcrecord.add("980", a=f001, b="130", c="VDEH")

    outputfile.write(marcrecord.as_marc())

inputfile.close()
outputfile.close()