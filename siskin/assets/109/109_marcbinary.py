#!/usr/bin/env python3
# coding: utf-8

# SID: 109
# Collection: Kunsthochschule für Medien Köln (VK Film)
# refs: #8391


import sys
import marcx
import xmltodict
import tarfile

tags = ["001", "002", "003", "004", "010", "020", "025", "026", "027", "036", "037",
        "038", "070", "071", "073", "076", "078", "080", "084", "085", "086", "087",
        "088", "089", "090", "100", "104", "108", "112", "116", "120", "124", "128",
        "132", "136", "140", "144", "148", "152", "156", "176", "200", "204", "208",
        "212", "216", "220", "224", "237", "300", "304", "310", "331", "333", "334",
        "335", "340", "341", "342", "343", "344", "345", "347", "349", "351", "353",
        "359", "360", "361", "365", "369", "370", "376", "400", "403", "405", "406",
        "407", "410", "412", "415", "417", "418", "425", "427", "431", "433", "434",
        "435", "437", "443", "451", "453", "454", "455", "456", "461", "462", "463",
        "464", "465", "466", "471", "473", "474", "475", "476", "481", "501", "502",
        "503", "504", "505", "507", "508", "509", "510", "511", "512", "513", "515",
        "516", "517", "518", "519", "523", "524", "525", "527", "529", "530", "531",
        "532", "533", "534", "536", "537", "538", "540", "542", "544", "545", "551",
        "553", "568", "570", "574", "580", "590", "591", "593", "594", "595", "596",
        "597", "598", "599", "651", "652", "653", "654", "655", "659", "670", "673",
        "674", "675", "700", "705", "710", "711", "720", "740", "750", "800", "802",
        "804", "805", "806", "808", "811", "812", "817", "818", "823", "824", "829",
        "902", "903", "904", "907", "908", "909", "913", "914", "UID"]


def get_datafield(tag, code):
    for field in datafield:        
        if field["@tag"] == tag:           
            if isinstance(field["subfield"], list):
                for subfield in field["subfield"]:
                    if subfield["@code"] == code:
                        return subfield["#text"]
            else:
                if field["subfield"]["@code"] == code:
                    return field["subfield"]["#text"]
    return ""


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
        if f245a == "":
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

        # Schlagwörter
        f650a = get_datafield("710", "a")
        marcrecord.add("650", a=f650a)
        f650a = get_datafield("711", "a")
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
