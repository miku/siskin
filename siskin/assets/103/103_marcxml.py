#!/usr/bin/env python
# coding: utf-8
# pylint: disable=C0103

"""
Note: This is WIP. Currently Python 2 only.

Input file is an OAI DC XML, processing currently tailored for SID 103.

    $ python 103.py INFILE OUTFILE

"""

from __future__ import print_function

import base64
import cgi
import io
import os
import re
import sqlite3
import sys
import tempfile

import xmltodict

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

inputfile = io.open(sys.argv[1], "r", encoding='utf-8')
outputfile = io.open(sys.argv[2], "w", encoding='utf-8')

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

inserts = 0
updates = 0

for i, record in enumerate(xml["collection"]["Record"]):

    if record["header"]["@status"] != "deleted":

        identifier = record["header"]["identifier"]

        try:
            title = record["metadata"]["oai_dc:dc"]["dc:title"]
            if isinstance(title, list):
                title = '||'.join(record["metadata"]["oai_dc:dc"]["dc:title"])
        except (TypeError, KeyError):
            title = ''

        try:
            description = record["metadata"]["oai_dc:dc"]["dc:description"]
            if isinstance(description, list):
                description = '||'.join(record["metadata"]["oai_dc:dc"]["dc:description"])
        except (TypeError, KeyError):
            description = ''

        try:
            subject = record["metadata"]["oai_dc:dc"]["dc:subject"]
            if isinstance(subject, list):
                subject = '; '.join(record["metadata"]["oai_dc:dc"]["dc:subject"])
        except (TypeError, KeyError):
            subject = ''

        try:
            type = record["metadata"]["oai_dc:dc"]["dc:type"]
            if isinstance(type, list):
                type = '||'.join(record["metadata"]["oai_dc:dc"]["dc:type"])
        except (TypeError, KeyError):
            type = ''

        try:
            format = record["metadata"]["oai_dc:dc"]["dc:format"]
            if isinstance(format, list):
                format = '||'.join(record["metadata"]["oai_dc:dc"]["dc:format"])
        except (TypeError, KeyError):
            format = ''

        try:
            relation = record["metadata"]["oai_dc:dc"]["dc:relation"]
            if isinstance(relation, list):
                relation = '||'.join(record["metadata"]["oai_dc:dc"]["dc:relation"])
        except (TypeError, KeyError):
            relation = ''

        try:
            publisher = record["metadata"]["oai_dc:dc"]["dc:publisher"]
            if isinstance(publisher, list):
                publisher = '||'.join(record["metadata"]["oai_dc:dc"]["dc:publisher"])
        except (TypeError, KeyError):
            publisher = ''

        try:
            date = record["metadata"]["oai_dc:dc"]["dc:date"]
            if isinstance(date, list):
                date = '||'.join(record["metadata"]["oai_dc:dc"]["dc:date"])
        except (TypeError, KeyError):
            date = ''

        try:
            source = record["metadata"]["oai_dc:dc"]["dc:source"]
            if isinstance(source, list):
                source = record["metadata"]["oai_dc:dc"]["dc:source"][0]
        except (TypeError, KeyError):
            source = ''

        try:
            language = record["metadata"]["oai_dc:dc"]["dc:language"]
            if isinstance(language, list):
                language = '||'.join(record["metadata"]["oai_dc:dc"]["dc:language"])
        except (TypeError, KeyError):
            language = ''

        try:
            rights = record["metadata"]["oai_dc:dc"]["dc:rights"]
            if isinstance(rights, list):
                rights = '||'.join(record["metadata"]["oai_dc:dc"]["dc:rights"])
        except (TypeError, KeyError):
            rights = ''

        try:
            url = record["metadata"]["oai_dc:dc"]["dc:identifier"]
            if isinstance(url, list):
                url = record["metadata"]["oai_dc:dc"]["dc:identifier"][1]
        except (TypeError, KeyError):
            url = ''

        try:
            creator = record["metadata"]["oai_dc:dc"]["dc:creator"]
            if isinstance(creator, list):
                creator = '||'.join(record["metadata"]["oai_dc:dc"]["dc:creator"])
        except (TypeError, KeyError):
            creator = ''

        try:
            coverage = record["metadata"]["oai_dc:dc"]["dc:coverage"]
            if isinstance(coverage, list):
                coverage = '||'.join(record["metadata"]["oai_dc:dc"]["dc:coverage"])
        except (TypeError, KeyError):
            coverage = ''

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

            inserts = inserts + 1

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

            updates = updates + 1

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

        #print("Inserts:", inserts, "Updates", updates)

query = "SELECT * FROM record"
sqlite.execute(query)
rows = sqlite.fetchall()

outputfile.write(u"<collection>")
outputfile.write(u"\n")

for row in rows:
    identifier = row[1]
    identifier = base64.b64encode(identifier.encode()).decode().rstrip("=")
    title = row[2]
    title = cgi.escape(title)
    description = row[3]
    description = cgi.escape(description)
    subject = row[4]
    subject = cgi.escape(subject)
    type = row[5]
    format = row[6]
    relation = row[7]
    publisher = row[8]
    publisher = cgi.escape(publisher)
    date = row[9]
    source = row[10]
    source = cgi.escape(source)
    language = row[11]
    rights = row[12]
    rights = cgi.escape(rights)
    url = row[13]
    creator = row[14]
    creator = cgi.escape(creator)
    coverage = row[15]
    coverage = cgi.escape(coverage)

    if identifier == "":
        continue

    # Zur Gewährleistung der korrekten Reihenfolge der Tags werden zunächst alle Variablen deklariert und erst dann die Datei geschrieben

    f001 = "\t\t<controlfield tag=\"001\">finc-103-%s</controlfield>\n" % identifier

    # 2017-06-16: Example rewrite of some XML:
    #
    # formatmaps = {
    #     'periodical': {
    #         '007': 'tu',
    #         '008': '130227n20uuuuuuxx uuupger c',
    #         'leader': 'nas',
    #         '935b' : 'cofz',
    #     },
    #     'ephemera': {'007': 'ta', 'leader': 'ckm', '935b' : 'foto'},
    #     'magazine cover': {'007': 'ta', 'leader': 'ckm', '935b' : 'foto'},
    #     'photograph': {'007': 'ta', 'leader': 'ckm', '935b' : 'foto'},
    #     'postcard': {'007': 'ta', 'leader': 'ckm', '935b' : 'foto'},
    #     'clipping': {'007': 'ta', 'leader': 'ckm', '935b' : 'foto'},
    #     'flier (printed matter)': {'007': 'kf', 'leader': 'nam'},
    #     'storyboard': {'007': 'kf', 'leader': 'nam'},
    #     'sheet music': {'007': 'qu', 'leader': 'ncs'},
    #     'correspondence': {'007': 'kf', 'leader': 'nam'},
    #     'pamphlet': {'007': 'kf', 'leader': 'nam'},
    #     'correspondence document': {'007': 'kf', 'leader': 'nam'},
    #     'lobby card': {'007': 'ta', 'leader': 'ckm', '935b' : 'foto'},
    #     'photomechanical print': {'007': 'kf', 'leader': 'nam'},
    #     'cigarette cards': {'007': 'kd', 'leader': 'nam'},
    #     'souvenir handkerchief box': {'007': 'ta', 'leader': 'ckm', '935b' : 'foto'},
    #     'scrapbook': {'007': 'cr', 'leader': 'nam', '935b' : 'cofz'},
    #     'slides (photographs)': {'007': 'ta', 'leader': 'ckm', '935b' : 'foto'},
    #     'program (document)': {'007': 'cr', 'leader': 'nam', '935b' : 'cofz'},
    #     'pressbook': {'007': 'cr', 'leader': 'nam', '935b' : 'cofz'},
    #     'autograph album': {'007': 'tu', 'leader': 'ntm', '935b' : 'handschr'},
    #     'monograph': {'007': 'cr', 'leader': 'nam', '935b' : 'cofz'},
    #     'drawing': {'007': 'kd', 'leader': 'nam'},
    # }

    # TODO:
    # format not found: pamphlet;
    # format not found: correspondence; document
    # format not found: flier (printed matter);
    # format not found: Monograph
    # format not found: ephemera;
    # format not found: Periodical
    # format not found:
    # format not found: document
    # if format not in formatmaps:
    #     print("format not found: %s" % format, file=sys.stderr)
    #     continue

    # def get_field_007(format='photograph'):
    #     """ Return XML for field 007 given a format. """
    #     return """<controlfield tag="007">%s</controlfield>""" % formatmaps[format]["007"]

    # def get_field_008(format='photograph'):
    #     if "008" not in formatmaps[format]:
    #         return ""
    #     return """
    #         <controlfield tag="008">%s</controlfield>
    #     """ % formatmaps[format]["008"]

    # def get_leader(format='photograph'):
    #     """ Return XML for leader given a format. """
    #     return "<leader>00000%s0000000000000000</leader>" % formatmaps[format]["leader"]

    # def get_field_935b(format='photograph'):
    #     """ Return XML for 935.b given a format. """
    #     if "935b" not in formatmaps[format]:
    #         return ""
    #     return """
    #         <datafield tag="935" ind1=" " ind2=" ">
    #             <subfield code="b">%s</subfield>
    #         </datafield>
    #     """ % formatmaps[format]["935b"]

    # leader = get_leader(format=format)
    # f007 = get_field_007(format=format)
    # f008 = get_field_008(format=format)
    # f935b = get_field_935b(format=format)

    # ElectronicJournal
    if format == "periodical":
        leader = "\t\t<leader>00000nas0000000000000000</leader>\n"
        f007 = "\t\t<controlfield tag=\"007\">tu</controlfield>\n"
        f008 = "\t\t<controlfield tag=\"008\">130227n20uuuuuuxx uuupger c</controlfield>\n"
        f935b = "\t\t<datafield tag=\"935\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"b\">cofz</subfield>\n\t\t</datafield>\n"

    # Photo (Image gibt es nicht)
    elif format == "ephemera":
        leader = "\t\t<leader>00000ckm0000000000000000</leader>\n"
        f007 = "\t\t<controlfield tag=\"007\">ta</controlfield>\n"
        f008 = ""
        f935b = "\t\t<datafield tag=\"935\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"b\">foto</subfield>\n\t\t</datafield>\n"

    # Photo
    elif format == "magazine cover":
        leader = "\t\t<leader>00000ckm0000000000000000</leader>\n"
        f007 = "\t\t<controlfield tag=\"007\">ta</controlfield>\n"
        f008 = ""
        f935b = "\t\t<datafield tag=\"935\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"b\">foto</subfield>\n\t\t</datafield>\n"

    # Photo
    elif format == "photograph":
        leader = "\t\t<leader>00000ckm0000000000000000</leader>\n"
        f007 = "\t\t<controlfield tag=\"007\">ta</controlfield>\n"
        f008 = ""
        f935b = "\t\t<datafield tag=\"935\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"b\">foto</subfield>\n\t\t</datafield>\n"

    # Photo (Image gibt es nicht)
    elif format == "postcard":
        leader = "\t\t<leader>00000ckm0000000000000000</leader>\n"
        f007 = "\t\t<controlfield tag=\"007\">ta</controlfield>\n"
        f008 = ""
        f935b = "\t\t<datafield tag=\"935\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"b\">foto</subfield>\n\t\t</datafield>\n"

    # Photo
    elif format == "clipping":
        leader = "\t\t<leader>00000ckm0000000000000000</leader>\n"
        f007 = "\t\t<controlfield tag=\"007\">ta</controlfield>\n"
        f008 = ""
        f935b = "\t\t<datafield tag=\"935\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"b\">foto</subfield>\n\t\t</datafield>\n"

    # Print
    elif format == "flier (printed matter)":
        leader = "\t\t<leader>00000nam0000000000000000</leader>\n"
        f007 = "\t\t<controlfield tag=\"007\">kf</controlfield>\n"
        f008 = ""
        f935b = ""

    # Print
    elif format == "storyboard":
        leader = "\t\t<leader>00000nam0000000000000000</leader>\n"
        f007 = "\t\t<controlfield tag=\"007\">kf</controlfield>\n"
        f008 = ""
        f935b = ""

    # MusicalScore (ElectronicMusicalScore gibt es nicht)
    elif format == "sheet music":
        leader = "\t\t<leader>00000ncs0000000000000000</leader>\n"
        f007 = "\t\t<controlfield tag=\"007\">qu</controlfield>\n"
        f008 = ""
        f935b = ""

    # Print
    elif format == "correspondence":
        leader = "\t\t<leader>00000nam0000000000000000</leader>\n"
        f007 = "\t\t<controlfield tag=\"007\">kf</controlfield>\n"
        f008 = ""
        f935b = ""

    # Print
    elif format == "pamphlet":
        leader = "\t\t<leader>00000nam0000000000000000</leader>\n"
        f007 = "\t\t<controlfield tag=\"007\">kf</controlfield>\n"
        f008 = ""
        f935b = ""

    # Print
    elif format == "correspondence document":
        leader = "\t\t<leader>00000nam0000000000000000</leader>\n"
        f007 = "\t\t<controlfield tag=\"007\">kf</controlfield>\n"
        f008 = ""
        f935b = ""

    # Photo
    elif format == "lobby card":
        leader = "\t\t<leader>00000ckm0000000000000000</leader>\n"
        f007 = "\t\t<controlfield tag=\"007\">ta</controlfield>\n"
        f008 = ""
        f935b = "\t\t<datafield tag=\"935\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"b\">foto</subfield>\n\t\t</datafield>\n"

    # Print
    elif format == "photomechanical print":
        leader = "\t\t<leader>00000nam0000000000000000</leader>\n"
        f007 = "\t\t<controlfield tag=\"007\">kf</controlfield>\n"
        f008 = ""
        f935b = ""

    # Drawing
    elif format == "cigarette cards":
        leader = "\t\t<leader>00000nam0000000000000000</leader>\n"
        f007 = "\t\t<controlfield tag=\"007\">kd</controlfield>\n"
        f008 = ""
        f935b = ""

    # Photo
    elif format == "souvenir handkerchief box":
        leader = "\t\t<leader>00000ckm0000000000000000</leader>\n"
        f007 = "\t\t<controlfield tag=\"007\">ta</controlfield>\n"
        f008 = ""
        f935b = "\t\t<datafield tag=\"935\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"b\">foto</subfield>\n\t\t</datafield>\n"

    # ElectronicBook
    elif format == "scrapbook":
        leader = "\t\t<leader>00000nam0000000000000000</leader>\n"
        f007 = "\t\t<controlfield tag=\"007\">cr</controlfield>\n"
        f008 = ""
        f935b = "\t\t<datafield tag=\"935\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"b\">cofz</subfield>\n\t\t</datafield>\n"

    # Photo
    elif format == "slides (photographs)":
        leader = "\t\t<leader>00000ckm0000000000000000</leader>\n"
        f007 = "\t\t<controlfield tag=\"007\">ta</controlfield>\n"
        f008 = ""
        f935b = "\t\t<datafield tag=\"935\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"b\">foto</subfield>\n\t\t</datafield>\n"

    # ElectronicBook
    elif format == "program (document)":
        leader = "\t\t<leader>00000nam0000000000000000</leader>\n"
        f007 = "\t\t<controlfield tag=\"007\">cr</controlfield>\n"
        f008 = ""
        f935b = "\t\t<datafield tag=\"935\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"b\">cofz</subfield>\n\t\t</datafield>\n"

    # ElectronicBook
    elif format == "pressbook":
        leader = "\t\t<leader>00000nam0000000000000000</leader>\n"
        f007 = "\t\t<controlfield tag=\"007\">cr</controlfield>\n"
        f008 = ""
        f935b = "\t\t<datafield tag=\"935\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"b\">cofz</subfield>\n\t\t</datafield>\n"

    # ElectronicManuscript
    elif format == "autograph album":
        leader = "\t\t<leader>00000ntm0000000000000000</leader>\n"
        f007 = "\t\t<controlfield tag=\"007\">tu</controlfield>\n"
        f008 = "\t\t<controlfield tag=\"008\">130227n20uuuuuuxx uuumger c</controlfield>\n"
        f935b = "\t\t<datafield tag=\"935\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"b\">handschr</subfield>\n\t\t</datafield>\n"

    # ElectronicBook
    elif format == "monograph":
        leader = "\t\t<leader>00000nam0000000000000000</leader>\n"
        f007 = "\t\t<controlfield tag=\"007\">cr</controlfield>\n"
        f008 = ""
        f935b = "\t\t<datafield tag=\"935\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"b\">cofz</subfield>\n\t\t</datafield>\n"

    # Drawing
    elif format == "drawing":
        leader = "\t\t<leader>00000nam0000000000000000</leader>\n"
        f007 = "\t\t<controlfield tag=\"007\">kd</controlfield>\n"
        f008 = ""
        f935b = ""

    # Photo
    else:
        leader = "\t\t<leader>00000ckm0000000000000000</leader>\n"
        f007 = "\t\t<controlfield tag=\"007\">ta</controlfield>\n"
        f008 = ""
        f935b = "\t\t<datafield tag=\"935\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"b\">foto</subfield>\n\t\t</datafield>\n"

    if language != "":
        f041 = language.replace("English", "eng")
        f041 = language.replace("German", "ger")
        f041 = language.replace("French", "fre")
        f041 = language.replace("Italian", "ita")
        f041 = "\t\t<datafield tag=\"041\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"a\">" + str(f041) + "</subfield>\n\t\t</datafield>\n"
    else:
        f041 = ""

    if creator != "":
        creator = creator.split(" ; ")
        f100 = creator[0]
        f100 = "\t\t<datafield tag=\"100\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"a\">" + str(f100) + "</subfield>\n\t\t</datafield>\n"
        if len(creator) > 1:
            f700 = []
            for creator_part in creator[1:]:
                creator_part = "\t\t<datafield tag=\"700\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"a\">" + \
                    str(creator_part) + "</subfield>\n\t\t</datafield>\n"
                f700.append(creator_part)
            f700 = "".join(f700)
        else:
            f700 = ""
    else:
        f100 = ""
        f700 = ""

    if title != "":
        f245 = "\t\t<datafield tag=\"245\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"a\">" + unicode(title) + "</subfield>\n\t\t</datafield>\n"
    else:
        f245 = ""

    # Chicago : Selig Polyscope Co.
    # Shurey's Publications
    if publisher != "":
        match = re.search("(.*)\s:\s(.*)", publisher)
        if match:
            place = match.group(1)
            place = "\n\t\t\t<subfield code=\"a\">" + str(place) + "</subfield>"
            publisher = match.group(2)
            publisher = "\n\t\t\t<subfield code=\"b\"> : " + unicode(publisher) + "</subfield>"
        else:
            place = ""
            publisher = "\n\t\t\t<subfield code=\"b\">" + unicode(publisher) + "</subfield>"
    else:
        publisher = ""
        place = ""

    # 1932-04-01
    #1930; 1931
    # 1922
    if date != "":
        match = re.search("(^\d\d\d\d)", date)
        if match:
            date = match.group(1)
            if publisher != "":
                date = "\n\t\t\t<subfield code=\"c\">, " + str(date) + "</subfield>"
            else:
                date = "\n\t\t\t<subfield code=\"c\">" + str(date) + "</subfield>"
        else:
            date = ""
    else:
        date = ""

    if place != "" or publisher != "" or date != "":
        f260 = "\t\t<datafield tag=\"260\" ind1=\" \" ind2=\" \">" + unicode(place) + unicode(publisher) + unicode(date) + "\n\t\t</datafield>\n"
    else:
        f260 = ""

    if source != "":
        f490 = "\t\t<datafield tag=\"490\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"a\">" + str(source) + "</subfield>\n\t\t</datafield>\n"
    else:
        f490 = ""

    if rights != "" and coverage != "":
        f500 = []
        f500.append("\t\t<datafield tag=\"500\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"a\">" + str(rights) + "</subfield>\n\t\t</datafield>\n")
        f500.append("\t\t<datafield tag=\"500\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"a\">" + str(coverage) + "</subfield>\n\t\t</datafield>\n")
        f500 = "".join(f500)
    elif rights != "":
        f500 = "\t\t<datafield tag=\"500\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"a\">" + str(rights) + "</subfield>\n\t\t</datafield>\n"
    elif coverage != "":
        f500 = "\t\t<datafield tag=\"500\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"a\">" + str(coverage) + "</subfield>\n\t\t</datafield>\n"
    else:
        f500 = ""

    if description != "":
        description = description.split("||")
        f520 = []
        for description_part in description:
            f520.append("\t\t<datafield tag=\"520\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"a\">" +
                        unicode(description_part) + "</subfield>\n\t\t</datafield>\n")
        f520 = "".join(f520)
    else:
        f520 = ""

    if subject != "" or format != "":
        # manchmal ended die Schlagwortfolgfe auf ;
        subject = subject.rstrip(";")
        # die Trennzeichen sind in der Quelle uneinheitlich: mal " ; " mal "; "
        # Problem: Semikolon ist Trennzeichen zwischen Schlagwörtern, kommt aber auch als Funktionszeichen in HTML-Entitities (&amp;) vor
        match = re.search("&\w\w\w?\w?;", subject)

        # wenn keine HTMl-Entities vorkommen, werden die Trennzeichen vereinheitlicht
        if not match:
            subject = subject.replace(" ; ", ";")
            subject = subject.replace(" ;", ";")
            subject = subject.replace("; ", ";")
            subject = subject.split(";")
        # wenn HTMl-Entities vorkommen, wird nur das reguläre Trennzeichen verwendet und abweichende Fälle bleiben unberücksichtigt
        else:
            subject = subject.split(" ; ")

        f689 = []
        for subject_part in subject:
            f689.append("\t\t<datafield tag=\"689\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"a\">" +
                        unicode(subject_part) + "</subfield>\n\t\t</datafield>\n")

        format = format.title()
        f689.append("\t\t<datafield tag=\"689\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"a\">" + str(format) + "</subfield>\n\t\t</datafield>\n")
        f689 = "".join(f689)

    else:
        f689 = ""

    # if relation != "":
     #   outputfile.write(u"\t\t<datafield tag=\"856\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"q\">text/html</subfield>\n\t\t\t<subfield code=\"3\">Link zur Reihe</subfield>\n\t\t\t<subfield code=\"u\">" + str(relation) + "</subfield>\n\t\t</datafield>\n")

    if url != "":
        url = url.split("||")
        if len(url) == 1:
            url = url[0]
            f856 = "\t\t<datafield tag=\"856\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"q\">text/html</subfield>\n\t\t\t<subfield code=\"3\">Link zur Ressource</subfield>\n\t\t\t<subfield code=\"u\">" + \
                str(url) + "</subfield>\n\t\t</datafield>\n"
        else:
            items = len(url)
            f520 = "\t\t<datafield tag=\"520\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"a\">Contains a collection of " + \
                str(items) + " Pictures.</subfield>\n\t\t</datafield>\n"
            f856 = []
            i = 1
            for url_part in url:
                f856.append("\t\t<datafield tag=\"856\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"q\">text/html</subfield>\n\t\t\t<subfield code=\"3\">Picture " +
                            str(i) + "</subfield>\n\t\t\t<subfield code=\"u\">" + str(url_part) + "</subfield>\n\t\t</datafield>\n")
                i = i + 1
            f856 = "".join(f856)
    else:
        f856 = ""

    f980 = "\t\t<datafield tag=\"980\" ind1=\" \" ind2=\" \">\n\t\t\t<subfield code=\"a\">%s</subfield>\n\t\t\t<subfield code=\"b\">103</subfield>\n\t\t</datafield>\n" % identifier

    outputfile.write(u"\t<record>\n")
    outputfile.write(unicode(leader))
    outputfile.write(unicode(f001))
    outputfile.write(unicode(f007))
    outputfile.write(unicode(f008))
    outputfile.write(unicode(f041))
    outputfile.write(unicode(f100))
    outputfile.write(unicode(f245))
    outputfile.write(unicode(f260))
    outputfile.write(unicode(f490))
    outputfile.write(unicode(f500))
    outputfile.write(unicode(f520))
    outputfile.write(unicode(f689))
    outputfile.write(unicode(f700))
    outputfile.write(unicode(f856))
    outputfile.write(unicode(f935b))
    outputfile.write(unicode(f980))
    outputfile.write(u"\t</record>\n")

outputfile.write(u"</collection>")

sqlitecon.commit()
sqlitecon.close()
inputfile.close()
outputfile.close()
os.remove(dbfile)
