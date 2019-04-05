#!/usr/bin/env python3
# coding: utf-8


# Quelle: Institut Ägyptologie
# SID: 70
# Tickets: #5246, #13232, #14359
# technicalCollectionID: sid-70-col-aegyptologie


import os
import sys
import re
import sqlite3

import marcx
from siskin.utils import marc_build_imprint


lang_map = {"de": "ger", "en": "eng", "fr": "fre", "ru": "rus", "it": "ita", "es": "spa", "af": "afr", "cs": "csb", "ar": "ara", "hu": "hung"}


def get_field(field):
    field = record[field]
    if field:
        return field
    else:
        return ""
    

inputfilename = "70_input.ctv6" 
outputfilename = "70_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

sqlitecon = sqlite3.connect(inputfilename)
outputfile = open(outputfilename, "wb")

query = """
    SELECT 
        Reference.ID as id,
        Title as title,
        Subtitle as subtitle,        
        ISBN as isbn,
        LanguageCode as language,
        PageCount as pages,
        PlaceOfPublication as place,
        Publisher.Name as publisher,
        Year as year,
        ReferenceType as format,
        Volume as volume,
        Periodical.Name as jtitle,
        GROUP_CONCAT(Author , "; " ) As authors,
        GROUP_CONCAT(Editor , "; " ) As editors,
        Signatur as callnumber,
        SeriesTitle.Name as series,
        PageRange as range
    FROM
        Reference
    LEFT JOIN
        Periodical ON (Periodical.ID = Reference.PeriodicalID)
    LEFT JOIN
        ReferencePublisher ON (ReferencePublisher.ReferenceID = Reference.ID)
    LEFT JOIN
        Publisher ON (Publisher.ID = ReferencePublisher.PublisherID)
    LEFT JOIN
        ReferenceAuthor ON (ReferenceAuthor.ReferenceID = Reference.ID)
    LEFT JOIN
        (SELECT
            ID as AuthorID,
            substr(COALESCE(LastName, '') || ', ' || COALESCE(FirstName, '') || ' ' || COALESCE(MiddleName, ''), 0) as Author
        FROM
            Person) ON (AuthorID = ReferenceAuthor.PersonID)
    LEFT JOIN
        ReferenceEditor ON (ReferenceEditor.ReferenceID = Reference.ID)
    LEFT JOIN
        (SELECT
            ID as EditorID,
            substr(COALESCE(LastName, '') || ', ' || COALESCE(FirstName, '') || ' ' || COALESCE(MiddleName, ''), 0) as Editor
        FROM
            Person) ON (EditorID = ReferenceEditor.PersonID)
    LEFT JOIN
        (SELECT
            GROUP_CONCAT(CallNumber , "; " ) AS Signatur,
            ReferenceID
        FROM
            Location
        GROUP BY
            ReferenceID) as Location2 ON (Location2.ReferenceID = Reference.ID)
    LEFT JOIN
        SeriesTitle ON (SeriesTitle.ID = Reference.SeriesTitleID)
    WHERE
        ReferenceType != "Contribution" AND
        Title NOT NULL
    GROUP BY
        Reference.ID
    """

sqlite = sqlitecon.cursor()    
sqlite.execute(query)

for i, record in enumerate(sqlite):

    # Volume nur bei Artikeln angeben, sonst identisch mit Erscheinungsjahr

    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False

    author_doublets = []

    # Formatzuweisung
    format = get_field(9)
    if format == "JournalArticle" or format == "NewspaperArticle":
        leader = "     naa  22        4500"       
        f935c = "text"    
    elif format == "Thesis":
        leader = "     cam  22        4500"      
        f935c = "hs"
    else:
        leader = "     cam  22        4500"      
        f935c = "lo"

    f007 = "tu"
    f935b = "druck"

    # Identifikator
    f001 = str(i)
    marcrecord.add("001", data="finc-70-" + f001)

    # Leader
    marcrecord.leader = leader

    # Format 007
    marcrecord.add("007", data=f007)

    # ISBN
    f020a = get_field(3)
    if f020a:
        match = re.search("([0-9xX-]{10,17})", f020a)
        if match:
            marcrecord.add("020", a=f020a)

    # ISSN
    f022a = get_field(3)
    if f022a:
        match = re.search("([0-9xX-]{9,9})", f022a)
        if match:
            marcrecord.add("022", a=f022a)

    # Sprache
    language = get_field(4)
    f041a = lang_map.get(language, "")
    if f041a != "":
        marcrecord.add("008", data="130227uu20uuuuuuxx uuu %s  c" % f041a)
        marcrecord.add("041", a=f041a)
    else:
        print("Die Sprache %s fehlt in der Lang_Map!" % language)

    # 1. Urheber
    persons = get_field(12)
    if persons:
        persons = persons.split(";")
        f100a = persons[0]
        f100a = f100a.strip()
        marcrecord.add("100", a=f100a)
        author_doublets.append(f100a)

    # Titel
    f245a = get_field(1)
    f245b = get_field(2)
    marcrecord.add("245", a=f245a, b=f245b)

    # Erscheinungsvermerk
    f260a = get_field(6)
    f260b = get_field(7)
    f260c = get_field(8) 
    subfields = marc_build_imprint(f260a, f260b, f260c)
    marcrecord.add("260", subfields=subfields)

    # Umfangsangabe
    f300a = get_field(5)
    match = re.search("\<c\>(\d+)\</c\>", f300a)
    if match:
        f300a = match.group(1)
        f300a = f300a + " S."
        marcrecord.add("300", a=f300a)

    # weitere Urheber
    persons = get_field(12)
    if persons:
        persons = persons.split(";")
        for person in persons[1:]:
            f700a = person.strip()
            if f700a in author_doublets:
                continue            
            marcrecord.add("700", a=f700a)
            author_doublets.append(f700a)

    # Herausgeber
    editors = get_field(13)
    if editors:
        editors = editors.split(";")
        editors = set(editors)  # wegen der Dopplungen
        for editor in editors:
            f700a = editor.strip()       
            marcrecord.add("700", a=f700a)

    # Verweis auf Zeitschrift
    if format == "JournalArticle" or format == "NewspaperArticle":
        f773t = get_field(11)
        year = get_field(8)
        volume = get_field(10)
        pages = get_field(16)
        match = re.search("\<os\>(\d+-\d+)\</os\>", pages)
        
        if match:
            pages = match.group(1)
            pages = ", S. " + pages
        else:
            pages = ""
        
        if f773t and (volume or year or pages):
            f773g = volume + "(" + year + ")" + pages
        else:
            f773g = ""

        marcrecord.add("773", t=f773t, g=f773g)

    # Link zur Bestandsinfo
    callnumber = get_field(14)
    if not callnumber:
        callnumber = "nicht verfügbar"
    marcrecord.add("856", q="text/html", _3="Link zur Bestandsinformation", u="http://www.gko.uni-leipzig.de/de/aegyptologisches-institut/bibliothek/informationen.html", z="Bestand der Bibliothek des Ägyptologischen Institus, bitte informieren Sie sich vor Ort. Signatur: " + callnumber)

    # Kollektion    
    collections = ["a", f001, "b", "70", "c", "sid-70-col-aegyptologie"]
    marcrecord.add("980", subfields=collections)      
    
    outputfile.write(marcrecord.as_marc())
   
sqlitecon.close()
outputfile.close()
