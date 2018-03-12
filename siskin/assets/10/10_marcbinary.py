#!/usr/bin/env python3
# coding: utf-8

import io
import re
import sys
import json

import marcx


def get_field(tag):
    try:
        value = jsonrecord[tag]
    except:
        value = ""
    return value


inputfilename = "10_input.json"
outputfilename = "10_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

outputfile = io.open(outputfilename, "wb")
inputfile = open(inputfilename, "r")

for line in inputfile:
    jsonobject = json.loads(line)
    jsonrecords = jsonobject["content"]["results"]

    for jsonrecord in jsonrecords:

        marcrecord = marcx.Record(force_utf8=True)
        marcrecord.strict = False

        format = get_field("original_format")       
        if "notated music" in format:
            leader = "     com  22        4500"
            f935c = "muno"        
        elif "web page" in format:
            leader = "     nmi  22        4500"
            f935c = "website"
        #elif "audio recording" in format:
        #    leader = "     nmi  22        4500"
        #    f935c = "muto"
        #elif "collection" in format:
        #    leader = "     nom  22        4500"
        #    f935c = ""
        elif "photo" in format:
            leader = "     ckm  22        4500"
            f935c = "foto"
        else:
            leader = "     cam  22        4500"
            f935c = "text"
        
        # Leader
        marcrecord.leader = leader

        # Format
        marcrecord.add("007", data="cr")

        #Identifikator
        f001 = jsonrecord["id"]
        f001 = f001.rstrip("/").split("/")[-1]
        marcrecord.add("001", data="finc-10-" + f001)

        # 1. Urheber
        f100a = get_field("contributor")
        if f100a != "":
            f100a = f100a[0]
            f100a = f100a.title()
            marcrecord.add("100", a=f100a)

        # Haupttitel
        f245a = get_field("title")
        marcrecord.add("245", a=f245a)

        # Alternativtitel
        atitles = get_field("other_title")
        for atitle in atitles:
            marcrecord.add("246", a=atitle)

        # Erscheinungsjahr
        f260c = get_field("date")
        marcrecord.add("260", c=f260c)

        # Schlagwörter
        subjects = get_field("subject")
        for subject in subjects:
            subject = subject.title()
            marcrecord.add("650", a=subject)
       
        # weitere Urheber
        persons = get_field("contributor")
        if persons != "":
            for person in persons[1:]:
                person = person.title()
                marcrecord.add("700", a=person)

        #n Link zum Datensatz
        f856u = get_field("url")       
        marcrecord.add("856", q="text/html", _3="Link zur Ressource", u=f856u)

        # Format
        marcrecord.add("935", b="cofz", c=f935c)

        # Kollektion
        marcrecord.add("980", a=f001, b="10", c="Music Treasures Consortium")

        outputfile.write(marcrecord.as_marc())

inputfile.close()
outputfile.close()