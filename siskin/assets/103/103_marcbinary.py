#!/usr/bin/env python3
# coding: utf-8
# pylint: disable=C0103

from __future__ import print_function

import base64
import io
import os
import re
import sqlite3
import sys
import tempfile
from builtins import bytes

import marcx
import xmltodict


def clear_format(format):
    if isinstance(format, list):
        format = format[0]
    format = format.strip()
    format = format.rstrip(";")
    format = format.lower()
    format = format.split("; ") # manchmal sind mehrere Formate angegeben
    format = format[0] # nur das zuerst angegebene Format wird genommen
    return format

def get_leader(format='photograph'):
    return "     %s  22        4500" % formatmaps[format]["leader"]

def get_field_008(format='photograph', f041_a="   ", f260_c="20uu"):
    if "008" not in formatmaps[format]:
        return "130227u%suuuuxx uuup%s  c" % (f260_c, f041_a)
    return "130227%s%suuuuxx uuup%s  c" % (formatmaps[format]["008"], f260_c, f041_a)

def get_field_935b(format='photograph'):
    if "935b" not in formatmaps[format]:
        return ""
    return formatmaps[format]["935b"]

langmap = {
    "English": "eng",
    "German": "ger",
    "French": "fre",
    "Italian": "ita",
    "Russian": "rus",
    "Spanish": "spa"
    }

formatmaps = {
    'periodical':
    {
        '008': 'n',
        'leader': 'nas',
        '935b' : 'cofz'
    },
    'ephemera':
    {
        'leader': 'ckm',
        '935b' : 'foto'
    },
    'magazine cover':
    {
        'leader': 'ckm',
        '935b' : 'foto'
    },
    'photograph':
    {
        'leader': 'ckm',
        '935b' : 'foto'
    },
    'postcard':
    {
        'leader': 'ckm',
        '935b' : 'foto'
    },
    'clipping':
    {
        'leader': 'ckm',
        '935b' : 'foto'
    },
    'flier (printed matter)':
    {
        'leader': 'nam'
    },
    'storyboard':
    {
        'leader': 'nam'
    },
    'sheet music':
    {
        'leader': 'ncs'
    },
    'correspondence':
    {
        'leader': 'nam'
    },
    'pamphlet':
    {
        'leader': 'nam'
    },
    'correspondence document':
    {
        '007': 'kf',
        'leader': 'nam'
    },
    'lobby card':
    {
        'leader': 'ckm',
        '935b' : 'foto'
    },
    'photomechanical print':
    {
        'leader': 'nam'
    },
    'cigarette cards':
    {
        'leader': 'nam'
    },
    'souvenir handkerchief box':
    {
        'leader': 'ckm',
        '935b' : 'foto'
    },
    'scrapbook':
    {
        'leader': 'nam',
        '935b' : 'cofz'
    },
    'slides (photographs)':
    {
        'leader': 'ckm',
        '935b' : 'foto'
    },
    'program (document)':
    {
        'leader': 'nam',
        '935b' : 'cofz'
    },
    'pressbook':
    {
        'leader': 'nam',
        '935b' : 'cofz'
    },
    'autograph album':
    {
        'leader': 'ntm',
        '935b' : 'handschr'
    },
    'monograph':
    {
        'leader': 'nam',
        '935b' : 'cofz'
    },
    'drawing':
    {
        'leader': 'nam'
    },
}

if len(sys.argv) < 3:
    print('usage: %s INFILE OUTFILE' % sys.argv[0], file=sys.stderr)
    sys.exit(1)

_, dbfile = tempfile.mkstemp(prefix='103-')

sqlitecon = sqlite3.connect(dbfile)
sqlite = sqlitecon.cursor()
sqlite.execute("DROP TABLE IF EXISTS record")

query = """
    CREATE TABLE
       'record' ('id' INTEGER PRIMARY KEY AUTOINCREMENT,
                 'identifier' TEXT,
                 'title' TEXT,
                 'description' TEXT,
                 'subject' TEXT,
                 'type' TEXT,
                 'format' TEXT,
                 'relation' TEXT,
                 'publisher' TEXT,
                 'date' TEXT,
                 'source' TEXT,
                 'language' TEXT,
                 'rights' TEXT,
                 'url' TEXT,
                 'creator' TEXT,
                 'coverage' TEXT)
"""

sqlite.execute(query)

inputfile = io.open(sys.argv[1], "r", encoding="utf-8")
outputfile = io.open(sys.argv[2], "wb")

xml = xmltodict.parse(inputfile.read())

identifier = ""
title = ""
description = ""
subject = ""
type = ""
format = ""
relation = ""
publisher = ""
date = ""
source = ""
language = ""
rights = ""
url = ""
creator = ""
coverage = ""

for record in xml["collection"]["Record"]:

    if record["header"]["@status"] != "deleted":

        identifier = record["header"]["identifier"]

        title = record.get("metadata").get("oai_dc:dc").get("dc:title", "")
        if isinstance(title, list):
            title = "||".join(title)

        description = record.get("metadata").get("oai_dc:dc").get("dc:description", "")
        if isinstance(description, list):
            description = "||".join(description)

        subject = record.get("metadata").get("oai_dc:dc").get("dc:subject", "")
        if isinstance(subject, list):
            subject = "; ".join(subject)

        type = record.get("metadata").get("oai_dc:dc").get("dc:type", "")
        if isinstance(type, list):
            type = "||".join(type)

        format = record.get("metadata").get("oai_dc:dc").get("dc:format", "")
        if isinstance(format, list):
            format = "||".join(format)

        relation = record.get("metadata").get("oai_dc:dc").get("dc:relation", "")
        if isinstance(relation, list):
            relation = "||".join(relation)

        publisher = record.get("metadata").get("oai_dc:dc").get("dc:publisher", "")
        if isinstance(publisher, list):
            publisher = "||".join(publisher)

        date = record.get("metadata").get("oai_dc:dc").get("dc:date", "")
        if isinstance(date, list):
            date = "||".join(date)

        source = record.get("metadata").get("oai_dc:dc").get("dc:source", "")
        if isinstance(source, list):
            source = source[0]

        language = record.get("metadata").get("oai_dc:dc").get("dc:language", "")
        if isinstance(language, list):
            language = "||".join(language)

        rights = record.get("metadata").get("oai_dc:dc").get("dc:rights", "")
        if isinstance(rights, list):
            rights = "||".join(rights)

        url = record.get("metadata").get("oai_dc:dc").get("dc:identifier", "")
        if isinstance(url, list):
            url = url[1]

        creator = record.get("metadata").get("oai_dc:dc").get("dc:creator", "")
        if isinstance(creator, list):
            creator = "||".join(creator)

        coverage = record.get("metadata").get("oai_dc:dc").get("dc:coverage", "")
        if isinstance(coverage, list):
            coverage = "||".join(coverage)

        values = (title, format, type)

        query = """
            SELECT
                id,
                url,
                COUNT(id) AS matches
            FROM
                record
            WHERE
                title = ? AND
                format = ? AND
                type = ?
        """

        sqlite.execute(query, values)
        row = sqlite.fetchone()

        id = row[0]
        url_old = row[1]
        matches = row[2]

        if matches == 0:

            values = (identifier, title, description, subject, type, format, relation, publisher, date, source, language, rights, url, creator, coverage)

            query = """
                INSERT INTO
                    record (identifier,
                            title,
                            description,
                            subject,
                            type,
                            format,
                            relation,
                            publisher,
                            date,
                            source,
                            language,
                            rights,
                            url,
                            creator,
                            coverage)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        else:

            url = url_old + "||" + url
            values = (url, id)

            query = """
                UPDATE
                    record
                SET
                    url = ?
                WHERE
                    id = ?
            """

        sqlite.execute(query, values)

        identifier = ""
        title = ""
        description = ""
        subject = ""
        type = ""
        format = ""
        relation = ""
        publisher = ""
        date = ""
        source = ""
        language = ""
        rights = ""
        url = ""
        creator = ""
        coverage = ""

query = "SELECT * FROM record"
sqlite.execute(query)
rows = sqlite.fetchall()

for row in rows:
    identifier = row[1]
    identifier = bytes(identifier, "utf-8")
    identifier = base64.b64encode(identifier)
    identifier = identifier.decode("utf-8").rstrip("=")
    title = row[2]
    description = row[3]
    subject = row[4]
    type = row[5]
    format = row[6]
    format = clear_format(format)
    relation = row[7]
    publisher = row[8]
    date = row[9]
    source = row[10]
    language = row[11]
    rights = row[12]
    url = row[13]
    creator = row[14]
    coverage = row[15]

    if identifier == "":
        continue

    if format not in formatmaps:
        print("Das Format %s fehlt in der Lookup-Tabelle. Default-Wert ist 'photograph'" % format, file=sys.stderr)
        format = "photograph"

    f001 = "finc-103-%s" % identifier
    leader = get_leader(format=format)

    if language != "":
        language = language.rstrip(";") # manchmal endet die Sprache auf ";"
        language = language.replace(" ", "") # manchmal gibt es Leerzeichen mittendrin oder am Ende
        language = language.split(";") # manchmal sind mehrere Sprache angegeben
        if isinstance(language, list):
            language = language[0] # nur die zuerst angegebene Sprache wird berücksichtig

        f041_a = langmap.get(language, "")

        if f041_a == "":
            print("Die Sprache %s fehlt in der Lookup-Tabelle!" % language)
    else:
        f041_a = ""

    f008 = get_field_008(format=format, f041_a=f041_a)

    if creator != "":
        creator = creator.split(" ; ")
        f100_a = creator[0]
        if len(creator) > 1:
            f700_a = []
            for creator_part in creator[1:]:
                f700_a.append(creator_part)
        else:
            f700_a = ""
    else:
        f100_a = ""
        f700_a = ""

    if title != "":
        f245_a = title
    else:
        f245_a = ""

    # Chicago : Selig Polyscope Co.
    # Shurey's Publications
    if publisher != "":
        match = re.search("(.*)\s:\s(.*)", publisher)
        if match:
            f260_a, f260_b = match.groups()
            f260_b = " : %s" % f260_b
        else:
            f260_a = ""
            f260_b = publisher
    else:
        f260_b = ""
        f260_a = ""

    # mögliche Muster: 1932-04-01 | 1930; 1931 | 1922
    if date != "":
        match = re.search("(^\d\d\d\d)", date)
        if match:
            f260_c = match.group(1)
            if publisher != "":
                f260_b = f260_b + ", "
        else:
            f260_c = ""
    else:
        f260_c = ""

    if source != "":
        f490_a = source
    else:
        f490_a = ""

    f500_a = []
    if rights != "":
        f500_a.append(rights)

    if coverage != "":
        f500_a.append(coverage)

    if rights == "" and coverage == "":
        f500_a = ""

    if description != "":
        description = description.split("||")
        f520_a = []
        for description_part in description:
            f520_a.append(description_part)
    else:
        f520_a = ""

    if subject != "" or format != "":
        # manchmal ended die Schlagwortfolgfe auf ;
        subject = subject.rstrip(";")

        # die Trennzeichen zwischen den Schlagwörtwern sind in der Quelle uneinheitlich: mal " ; " mal "; " mal " ;"
        subject = subject.replace(" ; ", ";")
        subject = subject.replace(" ;", ";")
        subject = subject.replace("; ", ";")
        subject = subject.split(";")

        f689_a = []
        for subject_part in subject:
            f689_a.append(subject_part)

        f689_a.append(format.title())

    else:
        f689_a = ""

    if url != "":
        url = url.split("||")
        f856_u = set(url)
        if len(f856_u) > 1:
            items = len(f856_u)
            f520_a = "Contains a collection of %s Pictures." % items

    #f935_b = get_field_935b(format=format)
    f980_a = identifier
    f980_b = "103"

    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False
    marcrecord.leader = leader
    marcrecord.add("001", data=f001)
    marcrecord.add("007", data="cr")
    f008 = get_field_008(format=format, f041_a=f041_a, f260_c=f260_c)
    marcrecord.add("008", data=f008)
    marcrecord.add("041", a=f041_a)
    marcrecord.add("100", a=f100_a)
    marcrecord.add("245", a=f245_a)
    publisher = ["a", f260_a, "b", f260_b, "c", f260_c]
    marcrecord.add("260", subfields=publisher)
    marcrecord.add("490", a=f490_a)

    for value in f500_a:
        marcrecord.add("500", a=value)

    if isinstance(f520_a, list):
        for value in f520_a:
            marcrecord.add("520", a=value)
    else:
        marcrecord.add("520", a=f520_a)

    for value in f689_a:
        marcrecord.add("689", a=value)

    for value in f700_a:
        marcrecord.add("700", a=value)

    if len(f856_u) > 1:
        for i, url in enumerate(f856_u, start=1):
            marcrecord.add("856", q="text/html", _3="Picture %s" % i, u=url)
    else:
        marcrecord.add("856", q="text/html", _3="Link zur Ressource", u=f856_u)

    #marcrecord.add("935", b=f935_b)
    marcrecord.add("980", a=f980_a, b=f980_b, c="Margaret Herrick Library")
    outputfile.write(marcrecord.as_marc())

sqlitecon.commit()
sqlitecon.close()
inputfile.close()
outputfile.close()
os.remove(dbfile)
