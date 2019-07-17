#!/usr/bin/env python
# coding: utf-8
#
# Copyright 2019 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
#                   Robert Schenk, <robert.schenk@uni-leipzig.de>
#                   Martin Czygan, <martin.czygan@uni-leipzig.de>
#
# This file is part of some open source application.
#
# Some open source application is free software: you can redistribute
# it and/or modify it under the terms of the GNU General Public
# License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.
#
# Some open source application is distributed in the hope that it will
# be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
#
# @license GPL-3.0+ <http://spdx.org/licenses/GPL-3.0+>

"""

Automated content verification of all sources in the (live) solr
Ticket: #15656

Usage:

    $ solrcheckup.py -d my.db -f 1.2.3.4:8080 -a solr.index.xyz --smtp-sender "beep@friendlyalarms.com"

This script should run fine in cron.
"""

import argparse
import io
import logging
import os
import re
import smtplib
import sqlite3
import sys
import tempfile
import time
from six.moves.urllib.parse import urlencode
from sqlite3 import Error
from siskin.mail import send_mail

import requests

logging.basicConfig(level=logging.DEBUG)

# The current database schema.
create_schema = """
    CREATE TABLE
        source
            (source INT PRIMARY KEY NOT NULL);

    CREATE TABLE
        institution
            (institution VARCHAR(30) PRIMARY KEY NOT NULL);

    CREATE TABLE
        sourcebyinstitution
            (sourcebyinstitution VARCHAR(30) PRIMARY KEY NOT NULL);

    CREATE TABLE
        history
            (date DEFAULT CURRENT_TIMESTAMP,
            sourcebyinstitution VARCHAR(30) NOT NULL,
            titles INT NOT NULL);
"""

# XXX: Encapsulate this better, to get rid of globals.
smtp_server = "mail.example.com" # XXX: use generic config for this
smtp_port = 465 # XXX: use generic config for this
smtp_name = ""
smtp_password = ""

smtp_sender = "noreply@example.com"
recipients = ["a@example.com", "b@example.com"]


def send_message(message):
    """
    Send e-mail to preconfigured recipients.
    """
    if not recipients:
        logging.warn("no recipients set, not sending any message")
        return

    send_mail(sender=smtp_sender,
              tolist=recipients,
              subject="SolrCheckup Warnung!",
              message=message,
              smtp=smtp_server,
              smtp_port=smtp_port,
              username=smtp_name,
              password=smtp_password)


def create_connection_and_set_cursor(database):
    """
    Creates a database connection to a SQLite database and returns a cursor
    """
    try:
        conn = sqlite3.connect(database)
    except Error as e:
        logging.error(e)
        sys.exit("No database connection could be established.")
    cursor = conn.cursor()
    return (conn, cursor)


def get_solr_result(index, params):
    """
    Takes a Solr index and a dict of parameters and returns a result object.
    Index should be hostport or ip:port, like 10.1.1.1:8085.
    """
    params = urlencode(params)
    result = requests.get("http://%s/solr/biblio/select?%s" % (index, params))
    return result.json()


def get_all_current_sources(k10plus, ai):
    """
    Get all current sources from Solr in both k10plus main index and ai.
    """
    params = {
        "facet": "true",
        "facet.field": "source_id",
        "facet.mincount": 3, # because of these cases ["", 2, "\" \"", 1]
        "q": "!source_id:error",
        "rows": 0,
        "wt": "json",
    }

    result = get_solr_result(k10plus, params)
    k10plus_sources = result["facet_counts"]["facet_fields"]["source_id"]
    k10plus_sources = set([int(sid) for sid in k10plus_sources[::2]])

    result = get_solr_result(ai, params)
    ai_sources = result["facet_counts"]["facet_fields"]["source_id"]
    ai_sources = set([int(sid) for sid in ai_sources[::2]])

    shared = k10plus_sources.intersection(ai_sources)
    if len(shared) > 0:
        ssid = [str(sid) for sid in shared]
        message = "Die folgenden Quellen befinden sich sowohl im K10plus als auch im AI: {}".format(", ".join(ssid))
        send_message(message)

    return k10plus_sources.union(ai_sources)


def get_all_old_sources(conn, sqlite):
    """
    Get all old sources from the Database.
    """
    query = """
        SELECT
            source
        FROM
            source
        GROUP BY
            source
    """

    sqlite.execute(query)
    old_sources = []

    for record in sqlite:
        old_source = record[0]
        old_sources.append(old_source)

    return old_sources


def update_sources(conn, sqlite, k10plus, ai):
    """
    Update the source table.
    """
    current_sources = get_all_current_sources(k10plus, ai)
    old_sources = get_all_old_sources(conn, sqlite)

    # Check if the source table is allready filled and this is not the first checkup
    source_table_is_filled = len(old_sources) > 100

    for old_source in old_sources:
        if source_table_is_filled and old_source not in current_sources:
            message = "Die SID %s ist im aktuellen Import nicht mehr vorhanden.\nWenn dies beabsichtigt ist, bitte die SID aus der Datenbank loeschen." % old_source
            send_message(message)

    for current_source in current_sources:
        if current_source not in old_sources:
            message = "The source %s is new in Solr." % current_source
            if source_table_is_filled:
                send_message(message)
            else:
                logging.info(message)
            sql = "INSERT INTO source (source) VALUES (%s)" % current_source
            sqlite.execute(sql)
            conn.commit()


def get_all_current_institutions(k10plus, ai):
    """
    Get all current institutions from Solr.
    """
    current_institutions = set()

    params = {
        "facet": "true",
        "facet.field": "institution",
        "facet.mincount": 3, # because of these cases ["", 2, "\" \"", 1]
        "q": "!source_id:error",
        "rows": 0,
        "wt": "json",
    }

    for index in (k10plus, ai):
        result = get_solr_result(index, params)
        institutions = result["facet_counts"]["facet_fields"]["institution"]
        for institution in institutions[::2]:
            current_institutions.add(institution)

    return current_institutions


def get_all_old_institutions(conn, sqlite):
    """
    Get all old institutions from the SQLite database.
    """
    query = """
        SELECT
            institution
        FROM
            institution
        GROUP BY
            institution
    """

    sqlite.execute(query)
    old_institutions = []

    for record in sqlite:
        old_institution = record[0]
        old_institutions.append(old_institution)

    return old_institutions


def get_all_old_sourcebyinstitutions(conn, sqlite):
    """
    Get all old sourcebyinstitution from the SQLite database.
    """
    query = """
        SELECT
            sourcebyinstitution
        FROM
            sourcebyinstitution
        GROUP BY
            sourcebyinstitution
    """

    sqlite.execute(query)
    old_sourcebyinstitutions = []

    for record in sqlite:
        old_sourcebyinstitution = record[0]
        old_sourcebyinstitutions.append(old_sourcebyinstitution)

    return old_sourcebyinstitutions


def get_old_sourcebyinstitution_number(conn, sqlite, sourcebyinstitution):
    """
    Get all the old sourcebyinstitution number from the SQLite database.
    """
    query = """
        SELECT
            titles
        FROM
            history
        WHERE
            sourcebyinstitution = "%s"
        ORDER BY
            titles DESC
        LIMIT 1
    """ % sourcebyinstitution

    sqlite.execute(query)
    for record in sqlite:
        old_sourcebyinstitution_number = record[0]
        return old_sourcebyinstitution_number


def update_institutions(conn, sqlite, k10plus, ai):
    """
    Update the institution table.
    """
    current_institutions = get_all_current_institutions(k10plus, ai)
    old_institutions = get_all_old_institutions(conn, sqlite)

    # Check if the institution table is allready filled and this is not the first checkup
    institution_table_is_filled = len(old_institutions) > 10

    for old_institution in old_institutions:
        if institution_table_is_filled and old_institution not in current_institutions:
            message = "Die ISIL %s ist im aktuellen Import nicht mehr vorhanden.\nWenn dies beabsichtigt ist, bitte die Institution aus der Datenbank loeschen." % old_institution
            send_message(message)

    for current_institution in current_institutions:
        if current_institution == " " or '"' in current_institution:
                continue
        if current_institution not in old_institutions:
            message = "The institution %s is new in Solr." % current_institution
            if institution_table_is_filled:
                send_message(message)
            else:
                logging.info(message)
            sql = "INSERT INTO institution (institution) VALUES ('%s')" % current_institution
            sqlite.execute(sql)
            conn.commit()


def update_history_and_sourcebyinstitution(conn, sqlite, k10plus, ai):
    """
    Get all current sources and title numbers from Solr and log them into database.
    """
    current_sources = get_all_current_sources(k10plus, ai)
    current_institutions = get_all_current_institutions(k10plus, ai)
    old_sourcebyinstitutions = get_all_old_sourcebyinstitutions(conn, sqlite)
    current_sourcebyinstitutions = []

    for source in current_sources:

        for institution in current_institutions:

            if not institution or institution == " " or '"' in institution:
                continue

            sourcebyinstitution = "SID " + str(source) + " (" + institution + ")"
            current_sourcebyinstitutions.append(sourcebyinstitution)

            params = {
                "q": 'source_id:%s AND institution:"%s"' % (source, institution),
                "rows": 0,
                "wt": "json"
            }

            # check k10plus
            result = get_solr_result(k10plus, params)
            number = result["response"]["numFound"]
            if number != 0:
                sql = 'INSERT INTO history (sourcebyinstitution, titles) VALUES ("%s", %s)' % (sourcebyinstitution, number)
                sqlite.execute(sql)
                conn.commit()
            else:
                # check ai
                result = get_solr_result(ai, params)
                number = result["response"]["numFound"]
                if number != 0:
                    # TODO: escape via sqlite
                    sql = 'INSERT INTO history (sourcebyinstitution, titles) VALUES ("%s", %s)' % (sourcebyinstitution, number)
                    sqlite.execute(sql)
                    conn.commit()

            if sourcebyinstitution not in old_sourcebyinstitutions:
                logging.info("The %s is now connected to SID %s.", institution, source)
                sql = "INSERT INTO sourcebyinstitution (sourcebyinstitution) VALUES ('%s')" % sourcebyinstitution
                sqlite.execute(sql)
                conn.commit()

            if number != 0:
                old_sourcebyinstitution_number = get_old_sourcebyinstitution_number(conn, sqlite, sourcebyinstitution)
                if number < old_sourcebyinstitution_number:
                    message = "Die Anzahl der Titel hat sich bei %s gegenueber einem frueheren Import verringert." % (sourcebyinstitution)
                    send_message(message)

            # requests.exceptions.ConnectionError: HTTPConnectionPool(XXXXXX): Max retries exceeded
            time.sleep(0.25)

    for old_sourcebyinstitution in old_sourcebyinstitutions:
        if old_sourcebyinstitution not in current_sourcebyinstitutions:
            message = "Die %s ist nicht laenger für die SID %s angesigelt." % (institution, source)
            send_message(message)

# Parse keyword arguments
parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("-v",
                    action="version",
                    help="show version",
                    version="0.0.1")
parser.add_argument("-d",
                    dest="database",
                    help="path to database",
                    default=os.path.join(tempfile.gettempdir(), "solrcheckup.sqlite"),
                    metavar="database")
parser.add_argument("-y",
                    dest="yaml",
                    help="link to review.yaml or another yaml template",
                    metavar="yaml")
parser.add_argument("-t",
                    dest="token",
                    help="private token for GitLab",
                    metavar="token")
parser.add_argument("-k",
                    dest="k10plus",
                    help="url of the k10plus index",
                    metavar="k10plus")
parser.add_argument("-a",
                    dest="ai",
                    help="url of the ai index",
                    metavar="ai",
                    required=True)
parser.add_argument("-n", "--smtp-name",
                    dest="smtp_name",
                    help="the login name fpr the email account",
                    metavar="smtp-name")
parser.add_argument("-p", "--smtp-password",
                    dest="smtp_password",
                    help="the password of the email account",
                    metavar="smtp_password")
parser.add_argument("--smtp-server",
                    dest="smtp_server",
                    help="SMTP server",
                    metavar="smtp_server")
parser.add_argument("--smtp-port",
                    dest="smtp_port",
                    help="SMTP port",
                    metavar="smtp_port",
                    default=465)
parser.add_argument("--smtp-sender",
                    dest="smtp_sender",
                    help="SMTP from address",
                    metavar="smtp_sender",
                    default="noreply@example.com")
parser.add_argument("--recipients",
                    dest="recipients",
                    help="recipients for alert messages, comma separated",
                    default="",
                    metavar="recipients")

args = parser.parse_args()

# XXX: Reduce use of globals.
smtp_server = args.smtp_server
smtp_port = args.smtp_port
smtp_server = args.smtp_server
smtp_sender = args.smtp_sender
smtp_name = args.smtp_name
smtp_password = args.smtp_password
recipients = [addr.strip() for addr in args.recipients.split(",") if addr.strip()]

database = args.database

# Exit when using yaml template without private token
yaml = args.yaml
token = args.token
if yaml and not token:
    sys.exit("Keyword argument for private token needed when using yaml template.")

# Ensure that all three indicies are specified
k10plus = args.k10plus
ai = args.ai

# Check if database already exists, otherwise create new one
if not os.path.isfile(database):
    conn, sqlite = create_connection_and_set_cursor(database)
    sqlite.executescript(create_schema)
else:
    conn, sqlite = create_connection_and_set_cursor(database)

# 1. Step: Update the source table
update_sources(conn, sqlite, k10plus, ai)

# 2. Step: Update the institution table
update_institutions(conn, sqlite, k10plus, ai)

# 3. Step: Get the number of titles for each SID and log them to database
update_history_and_sourcebyinstitution(conn, sqlite, k10plus, ai)

sqlite.close()
