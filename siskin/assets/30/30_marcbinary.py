#!/usr/bin/env python3
# coding: utf-8

"""
Issues: refs #7462, #12737, #13168, #13234, #13261.

This source is manually profiled for ADLR.
"""

from __future__ import print_function

import io
import re
import sys

import xmltodict
import marcx

relevant_sets = ["com_community_1080400", "com_community_10800", "col_collection_1080401", "col_collection_1080402", "col_collection_1080403",
                 "col_collection_1080404", "col_collection_1080405", "col_collection_1080406", "col_collection_1080407", "col_collection_1080408",
                 "col_collection_1080409", "col_collection_1080410", "col_collection_1080411", "col_collection_1080412", "col_collection_10800",
                 "col_collection_1080400", "col_collection_10899"]

collection_map = {
    "com_community_1080400": u"Massenkommunikation",
    "com_community_10800": u"Kommunikationswissenschaften",
    "col_collection_1080401": u"Rundfunk, Telekommunikation",
    "col_collection_1080402": u"Druckmedien",
    "col_collection_1080403": u"Andere Medien",
    "col_collection_1080404": u"Interaktive, elektronische Medien",
    "col_collection_1080405": u"Medieninhalte, Aussagenforschung",
    "col_collection_1080406": u"Kommunikatorforschung, Journalismus",
    "col_collection_1080407": u"Wirkungsforschung, Rezipientenforschung",
    "col_collection_1080408": u"Meinungsforschung",
    "col_collection_1080409": u"Werbung, Public Relations, Öffentlichkeitsarbeit",
    "col_collection_1080410": u"Medienpädagogik",
    "col_collection_1080411": u"Medienpolitik, Informationspolitik, Medienrecht",
    "col_collection_1080412": u"Medienökonomie, Medientechnik",
    "col_collection_10800": u"Kommunikationswissenschaften",
    "col_collection_1080400": u"Massenkommunikation (allgemein)",
    "col_collection_10899": u"Sonstiges zu Kommunikationswissenschaften"
}


def get_field(tag):
    try:
        return xmlrecord["metadata"]["oai_dc:dc"][tag]
    except:
        return ""


inputfilename = "30_input.xml"
outputfilename = "30_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = open(inputfilename, "rb")
outputfile = open(outputfilename, "wb")

xmlfile = inputfile.read()
xmlrecords = xmltodict.parse(xmlfile)

for xmlrecord in xmlrecords["Records"]["Record"]:

    marcrecord = marcx.Record(to_unicode=True, force_utf8=True)

    # Format
    format = get_field("dc:type")
    if "Monographie" in format or "Sammelwerk" in format or "Konferenzband" in format:
        leader = "     cam  22        4500"
        f935b = "cofz"
        f935c = ""
    if "Sammelwerksbeitrag" in format:
        leader = "     naa  22        4500"
        f935b = "cofz"
        f935c = ""
    elif "Diplomarbeit" in format or "Habilitationsschrift" in format or "Magisterarbeit" in format or "Dissertation" in format:
        leader = "     cam  22        4500"
        f935b = "cofz"
        f935c = "hs"
    else:
        leader = "     naa  22        4500"
        f935b = "cofz"
        f935c = ""

    # Leader
    marcrecord.leader = leader

    # Identifier
    f001 = xmlrecord["header"]["identifier"]
    regexp = re.match("oai:gesis\.izsoz\.de:document\/(\d+)", f001)
    if regexp:
        f001 = regexp.group(1)
        marcrecord.add("001", data="finc-30-" + f001)
    else:
        print("Fehler! Der Identifikator konnte nicht extrahiert werden: " + f001)

    # 007
    marcrecord.add("007", data="cr")

    marcrecord.strict = False

    # ISBN
    identifiers = get_field("dc:identifier")
    for identifier in identifiers:
        if isinstance(identifier, str):
            regexp = re.search("^(978-.*)", identifier)
            if regexp:
                f020a = regexp.group(1)
                marcrecord.add("020", a=f020a)
                break

    # ISSN
    identifiers = get_field("dc:identifier")
    for identifier in identifiers:
        if identifier is None:
            print('skipping None id', file=sys.stderr)
            continue
        regexp = re.search("^(\d\d\d\d-\d\d\d\d)", identifier)
        if regexp:
            f022a = regexp.group(1)
            marcrecord.add("022", a=f022a)
            break

    # Sprache
    """ Ist nur zweimal enthalten bei über 45.000 Titeln und einmal davon fehlerhaft, deshalb wird sie hier komplett weggelassen. """

    # 1. geistiger Schöpfer
    authors = get_field("dc:creator")
    if isinstance(authors, list):
        marcrecord.add("100", a=authors[0], e="aut")
    else:
        marcrecord.add("100", a=authors, e="aut")

    # Haupttitel
    f245 = get_field("dc:title")
    # Muss mit rein, weil an einer Stelle ein falscher, zusätzlicher title-Tag vorkommt.
    if isinstance(f245, list):
        f245 = f245[0]
    f245 = f245.split(" : ")
    if len(f245) > 1:
        f245a = f245[0]
        f245b = f245[1:]
    else:
        f245a = f245[0]
        f245b = ""
    marcrecord.add("245", a=f245a, b=f245b)

    # Erscheinungsvermerk
    # Die Angaben im Feld publisher sind sehr inkonsistent in Inhalt, Anzahl und Reihenfolge.
    # Relativ sicher ist bloß: wenn das Feld viermal vorhanden ist, steht im ersten der Verlag und im letzten der Ort.
    # Und wenn das Feld dreimal vorhanden ist im letzten der Ort
    # In den anderen Fällen ist es so sehr Kraut und Rüben, dass hier bloß das Erscheinungsjahr angegeben und der Rest weggelassen wird.
    # Andenfalls fällt die Datenqualität zu sehr ab.
    f260 = get_field("dc:publisher")
    if isinstance(f260, list) and len(f260) == 4:
        f260a = f260[3]
        f260b = f260[0]
    elif isinstance(f260, list) and len(f260) == 3:
        f260a = f260[2]
        f260b = ""
    else:
        f260a = ""
        f260b = ""
    dates = get_field("dc:date")
    for date in dates:
        regexp = re.search("^(\d\d\d\d)$", date)
        if regexp:
            f260c = regexp.group(1)
            break
        else:
            f260c = ""
    if f260a != "" and f260b != "":
        f260a = f260a + " : "
    if (f260a != "" or f260b != "") and f260c != "":
        f260c = ", " + f260c
    publisher = ["a", f260a, "b", f260b, "c", f260c]
    marcrecord.add("260", subfields=publisher)

    # Reihen / Kollektion
    collections = xmlrecord["header"]["setSpec"]
    for collection in collections:
        f490a = collection_map.get(collection, "")
        marcrecord.add("490", a=f490a)

    # Rechte
    f500a = get_field("dc:rights")
    if isinstance(f500a, list):
        marcrecord.add("500", a="Rechtehinweis: " + f500a[0])
    else:
        marcrecord.add("500", a="Rechtehinweis: " + f500a)

    # Abstract
    f520a = get_field("dc:description")
    marcrecord.add("520", a=f520a)

    # Schlagwörter
    unique_subjects = []
    subjects = get_field("dc:subject")
    for subject in subjects:
        if subject:
            # damit werden die englischen Schlagwörter weitestgehend herausgefiltert
            if subject[0].isupper():
                if subject not in unique_subjects:  # weil die Schlagwörter sich teilweise wiederholen
                    marcrecord.add("650", a=subject)
                    unique_subjects.append(subject)
            else:
                if " " in subject:  # für solche Fälle: soziale Lage, internationaler Vergleich
                    s = subject.split(" ")
                    x = s[1]
                    if x.isupper():
                        if subject not in unique_subjects:  # weil die Schlagwörter sich teilweise wiederholen
                            marcrecord.add("650", a=subject)
                            unique_subjects.append(subject)

    # weitere geistige Schöpfer
    authors = get_field("dc:creator")
    if isinstance(authors, list):
        for author in authors[1:]:
            marcrecord.add("700", a=author, e="aut")

    # Mitwirkende
    contributors = get_field("dc:contributor")
    if isinstance(contributors, list):
        for contributor in contributors:
            marcrecord.add("700", a=contributor, e="ctb")

    # übergeordnetes Werk
    journal = ""
    volume = ""
    issue = ""
    pages = ""
    sources = get_field("dc:source")
    if "Zeitschriftenartikel" in format and len(sources) == 4:
        journal = sources[0]
        volume = sources[1]
        issue = sources[2]
        pages = sources[3]
        f773g = u"%s(%s)%s, S. %s" % (volume, f260c, issue, pages)
        marcrecord.add("773", a=journal, g=f773g)
    elif sources != "":
        for source in sources:
            regexp = re.search("^(\D+)", source)
            if regexp:
                journal = regexp.group(1)
                marcrecord.add("773", a=journal)
                break

    # Link zu Datensatz und Ressource
    identifiers = get_field("dc:identifier")
    urn = False
    ssoar = ""
    for identifier in identifiers:
        if identifier:  # Martin fragen, warum hier sonst "argument of type 'NoneType' is not iterable" kommt

            if "http://www.ssoar" in identifier:  # ist manchmal ausschließlich in der Form angegeben
                ssoar = identifier
            elif "urn:nbn" in identifier:
                marcrecord.add("856", q="text/html", _3="Link zum Datensatz",
                               u="http://nbn-resolving.de/" + identifier)
                # damit bevorzugt der Permalink angezeigt wird und nicht der Link auf http://www.ssoar ...
                urn = True
            elif ".pdf" in identifier:
                marcrecord.add("856", q="text/html",
                               _3="Link zur Ressource", u=identifier)

    if urn == False:
        # Muss sein, damit der Link zum Datensatz nur einmal angezeigt wird
        marcrecord.add("856", q="text/html", _3="Link zum Datensatz", u=ssoar)

    # Medientyp
    marcrecord.add("935", b=f935b, c=f935c)

    # Kollektion
    collection_with_adlr = ["a", f001, "b", "30", "c",
                            "SSOAR Social Science Open Access Repository", "c", "ssoarAdlr"]
    collection_without_adlr = ["a", f001, "b", "30",
                               "c", "SSOAR Social Science Open Access Repository"]
    collections = xmlrecord["header"]["setSpec"]
    for relevant_set in relevant_sets:
        if relevant_set in collections:
            marcrecord.add("980", subfields=collection_with_adlr)
            break
    else:
        marcrecord.add("980", subfields=collection_without_adlr)

    outputfile.write(marcrecord.as_marc())

inputfile.close()
outputfile.close()
