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
    format = format.lstrip()
    format = format.rstrip()
    format = format.rstrip(";")
    format = format.lower()
    format = format.split("; ")  # manchmal sind mehrere Formate angegeben
    if isinstance(format, list):
        format = format[0]  # nur das zuerst angegebe Format wird genommen
    return format


def get_leader(format='photograph'):
    return "     %s  22        450 " % formatmaps[format]["leader"]


def get_field_007(format='photograph'):
    return formatmaps[format]["007"]


def get_field_008(format='photograph', f041_a="   "):
    if "008" not in formatmaps[format]:
        return "130227uu20uuuuuuxx uuup%s  c" % f041_a
    return "130227%s20uuuuuuxx uuup%s  c" % (formatmaps[format]["008"], f041_a)


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
        '007': 'tu',
        '008': 'n',
        'leader': 'nas',
        '935b': 'cofz'
    },
    'ephemera':
    {
        '007': 'ta',
        'leader': 'ckm',
        '935b': 'foto'
    },
    'magazine cover':
    {
        '007': 'ta',
        'leader': 'ckm',
        '935b': 'foto'
    },
    'photograph':
    {
        '007': 'ta',
        'leader': 'ckm',
        '935b': 'foto'
    },
    'postcard':
    {
        '007': 'ta',
        'leader': 'ckm',
        '935b': 'foto'
    },
    'clipping':
    {
        '007': 'ta',
        'leader': 'ckm',
        '935b': 'foto'
    },
    'flier (printed matter)':
    {
        '007': 'kf',
        'leader': 'nam'
    },
    'storyboard':
    {
        '007': 'kf',
        'leader': 'nam'
    },
    'sheet music':
    {
        '007': 'qu',
        'leader': 'ncs'
    },
    'correspondence':
    {
        '007': 'kf',
        'leader': 'nam'
    },
    'pamphlet':
    {
        '007': 'kf',
        'leader': 'nam'
    },
    'correspondence document':
    {
        '007': 'kf',
        'leader': 'nam'
    },
    'lobby card':
    {
        '007': 'ta',
        'leader': 'ckm',
        '935b': 'foto'
    },
    'photomechanical print':
    {
        '007': 'kf',
        'leader': 'nam'
    },
    'cigarette cards':
    {
        '007': 'kd',
        'leader': 'nam'
    },
    'souvenir handkerchief box':
    {
        '007': 'ta',
        'leader': 'ckm',
        '935b': 'foto'
    },
    'scrapbook':
    {
        '007': 'cr',
        'leader': 'nam',
        '935b': 'cofz'
    },
    'slides (photographs)':
    {
        '007': 'ta',
        'leader': 'ckm',
        '935b': 'foto'
    },
    'program (document)':
    {
        '007': 'cr',
        'leader': 'nam',
        '935b': 'cofz'
    },
    'pressbook':
    {
        '007': 'cr',
        'leader': 'nam',
        '935b': 'cofz'
    },
    'autograph album':
    {
        '007': 'tu',
        'leader': 'ntm',
        '935b': 'handschr'
    },
    'monograph':
    {
        '007': 'cr',
        'leader': 'nam',
        '935b': 'cofz'
    },
    'drawing':
    {
        '007': 'kd',
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

        try:
            title = record["metadata"]["oai_dc:dc"]["dc:title"]
            if isinstance(title, list):
                title = "||".join(record["metadata"]["oai_dc:dc"]["dc:title"])
        except (TypeError, KeyError):
            title = ""

        try:
            description = record["metadata"]["oai_dc:dc"]["dc:description"]
            if isinstance(description, list):
                description = "||".join(record["metadata"]["oai_dc:dc"]["dc:description"])
        except (TypeError, KeyError):
            description = ""

        try:
            subject = record["metadata"]["oai_dc:dc"]["dc:subject"]
            if isinstance(subject, list):
                subject = "; ".join(record["metadata"]["oai_dc:dc"]["dc:subject"])
        except (TypeError, KeyError):
            subject = ""

        try:
            type = record["metadata"]["oai_dc:dc"]["dc:type"]
            if isinstance(type, list):
                type = "||".join(record["metadata"]["oai_dc:dc"]["dc:type"])
        except (TypeError, KeyError):
            type = ""

        try:
            format = record["metadata"]["oai_dc:dc"]["dc:format"]
            if isinstance(format, list):
                format = "||".join(record["metadata"]["oai_dc:dc"]["dc:format"])
        except (TypeError, KeyError):
            format = ""

        try:
            relation = record["metadata"]["oai_dc:dc"]["dc:relation"]
            if isinstance(relation, list):
                relation = "||".join(record["metadata"]["oai_dc:dc"]["dc:relation"])
        except (TypeError, KeyError):
            relation = ""

        try:
            publisher = record["metadata"]["oai_dc:dc"]["dc:publisher"]
            if isinstance(publisher, list):
                publisher = "||".join(record["metadata"]["oai_dc:dc"]["dc:publisher"])
        except (TypeError, KeyError):
            publisher = ""

        try:
            date = record["metadata"]["oai_dc:dc"]["dc:date"]
            if isinstance(date, list):
                date = "||".join(record["metadata"]["oai_dc:dc"]["dc:date"])
        except (TypeError, KeyError):
            date = ""

        try:
            source = record["metadata"]["oai_dc:dc"]["dc:source"]
            if isinstance(source, list):
                source = record["metadata"]["oai_dc:dc"]["dc:source"][0]
        except (TypeError, KeyError):
            source = ""

        try:
            language = record["metadata"]["oai_dc:dc"]["dc:language"]
            if isinstance(language, list):
                language = "||".join(record["metadata"]["oai_dc:dc"]["dc:language"])
        except (TypeError, KeyError):
            language = ""

        try:
            rights = record["metadata"]["oai_dc:dc"]["dc:rights"]
            if isinstance(rights, list):
                rights = "||".join(record["metadata"]["oai_dc:dc"]["dc:rights"])
        except (TypeError, KeyError):
            rights = ""

        try:
            url = record["metadata"]["oai_dc:dc"]["dc:identifier"]
            if isinstance(url, list):
                url = record["metadata"]["oai_dc:dc"]["dc:identifier"][1]
        except (TypeError, KeyError):
            url = ""

        try:
            creator = record["metadata"]["oai_dc:dc"]["dc:creator"]
            if isinstance(creator, list):
                creator = "||".join(record["metadata"]["oai_dc:dc"]["dc:creator"])
        except (TypeError, KeyError):
            creator = ""

        try:
            coverage = record["metadata"]["oai_dc:dc"]["dc:coverage"]
            if isinstance(coverage, list):
                coverage = "||".join(record["metadata"]["oai_dc:dc"]["dc:coverage"])
        except (TypeError, KeyError):
            coverage = ""

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
    f007 = get_field_007(format=format)

    if language != "":
        language = language.rstrip(";")  # manchmal endet die Sprache auf ";"
        language = language.replace(" ", "")  # manchmal gibt es Leerzeichen mittendrin oder am Ende
        language = language.split(";")  # manchmal sind mehrere Sprache angegeben
        if isinstance(language, list):
            language = language[0]  # nur die zuerst angegebene Sprache wird berücksichtig

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
                f260_c = ", %s" % f260_c
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
        if len(url) == 1:
            url = url[0]
            f856_u = url
        else:
            items = len(url)
            f520_a = "Contains a collection of %s Pictures." % items
            f856_u = []
            for url_part in url:
                f856_u.append(url_part)
    else:
        f856_u = ""

    f935_b = get_field_935b(format=format)
    f980_a = identifier
    f980_b = "103"

    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False
    marcrecord.leader = leader
    marcrecord.add("001", data=f001)
    marcrecord.add("007", data=f007)
    marcrecord.add("008", data=f008)
    marcrecord.add("041", a=f041_a)
    marcrecord.add("100", a=f100_a)
    marcrecord.add("245", a=f245_a)
    marcrecord.add("260", a=f260_a, b=f260_b, c=f260_c)
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

    if isinstance(f856_u, list):
        for i, url in enumerate(f856_u, start=1):
            marcrecord.add("856", q="text/html", _3="Picture %s" % i, u=url)
    else:
        marcrecord.add("856", q="text/html", _3="Link zur Ressource", u=f856_u)

    marcrecord.add("935", b=f935_b)
    marcrecord.add("980", a=f980_a, b=f980_b, c="Margaret Herrick Library")
    outputfile.write(marcrecord.as_marc())

sqlitecon.commit()
sqlitecon.close()
inputfile.close()
outputfile.close()
os.remove(dbfile)
