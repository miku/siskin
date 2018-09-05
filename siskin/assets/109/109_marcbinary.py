#!/usr/bin/env python3
# coding: utf-8

# SID: 109
# Collection: Kunsthochschule für Medien Köln (VK Film)
# refs: #8391


import sys
import re
import marcx
import xmltodict
import tarfile


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
    "Objekt":
    {
        "leader": "crm",
        "007": "zz",
        "935b": "gegenst"
    },
    "Sonstiges":
    {
        "leader": "npa",
        "007": "tu"
    }
}


def get_datafield(tag, code, all=False):
    """
    Return string value for (tag, code) or list of values if all is True.
    """
    values = []
    for field in datafield:        
        if field["@tag"] != tag:      
            continue
        if isinstance(field["subfield"], list):
            for subfield in field["subfield"]:
                if subfield["@code"] != code:
                    continue
                if not all:
                    return subfield["#text"]
                values.append(subfield["#text"])
        else:
            if field["subfield"]["@code"] == code:
                if not all:
                    return field["subfield"]["#text"]
                values.append(field["subfield"]["#text"])
    return values

def get_leader(format="Buch"):
    return "     %s  22        4500" % formatmap[format]["leader"]

def get_field_007(format="Buch"):
    return formatmap[format]["007"]

def get_field_935b(format="Buch"):
    if "935b" not in formatmap[format]:
        return ""
    return formatmap[format]["935b"]

def get_field_935c(format="Buch"):
    if "935c" not in formatmap[format]:
        return ""
    return formatmap[format]["935c"]

def remove_brackets(field):
    if isinstance(field, list) and len(field) == 0:
        field = ""
    return field.replace("<<", "").replace(">>", "")


inputfilename1 = "109_input1.tar.gz"
inputfilename2 = "109_input2.tar.gz"
outputfilename = "109_output.mrc"

if len(sys.argv) == 4:
    inputfilename1, inputfilename2, outputfilename = sys.argv[1:]

filenames = [inputfilename1, inputfilename2]
outputfile = open(outputfilename, "wb")

for filename in filenames:
    inputfile = tarfile.open(filename, "r:gz")
    records = inputfile.getmembers()

    for i, record in enumerate(records):
        if i == 1000:
            #break
            pass
        xmlrecord = inputfile.extractfile(record)
        xmlrecord = xmlrecord.read()
        xmlrecord = xmltodict.parse(xmlrecord)
        datafield = xmlrecord["OAI-PMH"]["ListRecords"]["record"]["metadata"]["record"]["datafield"]

        marcrecord = marcx.Record(force_utf8=True)
        marcrecord.strict = False

        # Format
        format = get_datafield("433", "a")
        format = str(format)
        regexp = re.search("S\.\s\d+\s?-\s?\d+", format)
        if ("S." in format or "Bl." in format or "Ill." in format or " p." in format or "XI" in format or "XV" in format
                           or "X," in format or "Bde." in format or ": graph" in format):
            format = "Buch"
        elif "CD" in format:
            format = "CD"
        elif "DVD" in format:
            format = "DVD"
        elif "Blu-ray" in format:
            format = "Blu-ray"
        elif "Videokassette" in format or "VHS" in format or "Min" in format:
            format = "Videokassette"
        elif "Losebl.-Ausg." in format:
            format = "Loseblattsammlung"
        elif regexp:
            format = "Aufsatz"
        elif ("Plakat" in format or "Kassette" in format or "Box" in format or "Karton" in format or "Postkarten" in format
                                 or "Teile" in format or "USB" in format or "Schachtel" in format or "Schautafel" in format
                                 or "Medienkombination" in format or "Tafel" in format or "Faltbl" in format or "Schuber" in format):
            format = "Objekt"
        else:
            #print("unbekannt:", format, "Default = Sonstiges")
            format = "Sonstiges"

        # Leader
        leader = get_leader(format=format)
        marcrecord.leader = leader

        # Identifier
        f001 = get_datafield("001", "a")
        marcrecord.add("001", data="finc-109-" + f001)

        # 007
        f007 = get_field_007(format=format)
        marcrecord.add("007", data=f007)

        # ISBN
        f020a = get_datafield("540", "a")
        marcrecord.add("020", a=f020a)
        f020a = get_datafield("570", "a")
        marcrecord.add("020", a=f020a)

        # Sprache
        f041a = get_datafield("037", "a")
        marcrecord.add("041", a=f041a)

        # 1. Urheber
        f100a = get_datafield("100", "a")        
        f100a = remove_brackets(f100a)
        marcrecord.add("100", a=f100a)    
        
        # Haupttitel & Verantwortlichenangabe
        f245a = get_datafield("331", "a")
        if not f245a:
            #print(f001, file=sys.stderr)
            continue
        f245a = remove_brackets(f245a)
        f245c = get_datafield("359", "a")
        f245 = ["a", f245a, "c", f245c]
        marcrecord.add("245", subfields=f245)
        
        # Erscheinungsvermerk
        f260a = get_datafield("410", "a")       
        f260b = get_datafield("412", "a")      
        f260b = remove_brackets(f260b)
        f260c = get_datafield("425", "a")
        f260 = ["a", f260a, "b", f260b, "c", f260c]
        marcrecord.add("260", subfields=f260)

        # Umfangsangabe
        f300a = get_datafield("433", "a")       
        f300a = remove_brackets(f300a)
        f300b = get_datafield("434", "a")
        f300 = ["a", f300a, "b", f300b]
        marcrecord.add("300", subfields=f300)       

        for f650a in set(get_datafield("710", "a", all=True)):
            f650a = remove_brackets(f650a)
            marcrecord.add("650", a=f650a)

        for f650a in set(get_datafield("711", "a", all=True)):
            f650a = remove_brackets(f650a)
            marcrecord.add("650", a=f650a)

        # weitere Urheber
        for tag in range(101, 200):
            f700a = get_datafield(str(tag), "a")           
            f700a = remove_brackets(f700a)
            marcrecord.add("700", a=f700a)

        # weitere Körperschaften
        for tag in range(200, 300):
            f710a = get_datafield(str(tag), "a")           
            f710a = remove_brackets(f710a)
            marcrecord.add("710", a=f710a)

        # Format
        f935b = get_field_935b(format=format)
        f935c = get_field_935c(format=format)
        marcrecord.add("935", b=f935b, c=f935c)
                    
        # Kollektion
        collection = ["a", f001, "b", "109", "c", "Kunsthochschule für Medien Köln", "c", "Verbundkatalog Film"]
        marcrecord.add("980", subfields=collection)

        outputfile.write(marcrecord.as_marc())

    inputfile.close()
outputfile.close()
