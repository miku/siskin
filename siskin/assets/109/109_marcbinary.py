#!/usr/bin/env python3
# coding: utf-8

# SID: 109
# Collection: Kunsthochschule für Medien Köln (VK Film)
# refs: #8391


import sys
import marcx
import xmltodict
import tarfile


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
            break
        xmlrecord = inputfile.extractfile(record)
        xmlrecord = xmlrecord.read()
        xmlrecord = xmltodict.parse(xmlrecord)
        datafield = xmlrecord["OAI-PMH"]["ListRecords"]["record"]["metadata"]["record"]["datafield"]

        marcrecord = marcx.Record(force_utf8=True)
        marcrecord.strict = False

        # Leader
        marcrecord.leader = "     nab  22        4500"

        # Identifier
        f001 = get_datafield("001", "a")
        marcrecord.add("001", data="finc-109-" + f001)

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
        marcrecord.add("100", a=f100a)    
        
        # Haupttitel & Verantwortlichenangabe
        f245a = get_datafield("331", "a")
        f245c = get_datafield("359", "a")
        f245 = ["a", f245a, "c", f245c]
        marcrecord.add("245", subfields=f245)
        if not f245a:
            #print(f001, file=sys.stderr)
            continue

        # Erscheinungsvermerk
        f260a = get_datafield("410", "a")
        f260b = get_datafield("412", "a")
        f260c = get_datafield("425", "a")
        f260 = ["a", f260a, "b", f260b, "c", f260c]
        marcrecord.add("260", subfields=f260)

        # Umfangsangabe
        f300a = get_datafield("433", "a")
        f300b = get_datafield("434", "a")
        f300 = ["a", f300a, "b", f300b]
        marcrecord.add("300", subfields=f300)       

        for f650a in set(get_datafield("710", "a", all=True)):
            marcrecord.add("650", a=f650a)

        for f650a in set(get_datafield("711", "a", all=True)):
            marcrecord.add("650", a=f650a)

        # weitere Urheber
        for tag in range(101, 200):
            f700a = get_datafield(str(tag), "a")
            marcrecord.add("700", a=f700a)

        # weitere Körperschaften
        for tag in range(200, 300):
            f710a = get_datafield(str(tag), "a")
            marcrecord.add("710", a=f710a)
                    
        # Kollektion
        collection = ["a", f001, "b", "109", "c", "Kunsthochschule für Medien Köln", "c", "Verbundkatalog Film"]
        marcrecord.add("980", subfields=collection)

        outputfile.write(marcrecord.as_marc())

    inputfile.close()
outputfile.close()
