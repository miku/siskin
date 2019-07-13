#!/usr/bin/env python
# coding: utf-8
#
# Copyright 2019 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
#                   Martin Czygan, <martin.czygan@uni-leipzig.de>
#                   Robert Schenk, <robert.schenk@uni-leipzig.de>
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

Automated content verification of all sources in the live solr
Ticket: #15656

"""

from __future__ import print_function

import io
import os
import re
import sys
import time

import requests
import argparse
import sqlite3

from sqlite3 import Error


def create_connection_and_set_cursor(database):
    """
    Creates a database connection to a SQLite database and returns a cursor
    """
    try:
        conn = sqlite3.connect(database)
    except Error as e:
        print(e)
        sys.exit("No database connection could be established.")
    cursor = conn.cursor()
    return [conn, cursor]


def get_all_current_sources(conn, sqlite, finc, ai):
    """
    Get all current sources from Solr.
    """
    current_sources = []

    # check finc main index
    resp = requests.get("http://" + finc + "/solr/biblio/select?q=!source_id%3Aerror&rows=0&fl=source_id&wt=json&indent=true&facet=true&facet.field=source_id&facet.mincount=1")
    resp = resp.json()  
    finc_sources = resp["facet_counts"]["facet_fields"]["source_id"]
    for finc_source in finc_sources[::2]:
        finc_source = int(finc_source)
        current_sources.append(finc_source)

     # check ai index
    resp = requests.get("http://" + ai + "/solr/biblio/select?q=!source_id%3Aerror&rows=0&fl=source_id&wt=json&indent=true&facet=true&facet.field=source_id&facet.mincount=1")
    resp = resp.json()  
    ai_sources = resp["facet_counts"]["facet_fields"]["source_id"]
    for ai_source in ai_sources[::2]:
        ai_source = int(ai_source)
        if ai_source in current_sources:
            print("source %s is both in the finc main and in the ai index." % ai_source)
            continue
        current_sources.append(ai_source)
    
    return current_sources


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


def update_sources(conn, sqlite, finc, k10plus, ai):
    """
    Update the source table.
    """    
    current_sources = get_all_current_sources(conn, sqlite, finc, ai)
    old_sources = get_all_old_sources(conn, sqlite)

    # Check if the source table is allready filled and this is not the first checkup
    if len(old_sources) > 100:
        source_table_is_filled = True
    else:
        source_table_is_filled = False

    for old_source in old_sources:
        if source_table_is_filled and old_source not in current_sources:
            print("The source %s is no longer in Solr." % old_source)
            print("Please delete it from the source table if this change is permanent.")

    for current_source in current_sources:
        if current_source not in old_sources:
            print("The source %s is new in Solr." % current_source)
            sql = "INSERT INTO source (source) VALUES (%s)" % current_source
            sqlite.execute(sql)
            conn.commit()


def get_all_current_institutions(conn, sqlite, finc, ai):
    """
    Get all current institutions from Solr.
    """
    current_institutions = []

    # check finc main index
    resp = requests.get("http://" + finc + "/solr/biblio/select?q=!source_id%3Aerror&rows=0&fl=institution&wt=json&indent=true&facet=true&facet.field=institution&facet.mincount=1")
    resp = resp.json()  
    institutions = resp["facet_counts"]["facet_fields"]["institution"]
    for institution in institutions[::2]:
        current_institutions.append(institution)

     # check ai index
    resp = requests.get("http://" + finc + "/solr/biblio/select?q=!source_id%3Aerror&rows=0&fl=institution&wt=json&indent=true&facet=true&facet.field=institution&facet.mincount=1")
    resp = resp.json()  
    institutions = resp["facet_counts"]["facet_fields"]["institution"]
    for institution in institutions[::2]:        
        if institution in current_institutions:
            continue
        current_institutions.append(institution)
    
    return current_institutions


def get_all_old_institutions(conn, sqlite):
    """
    Get all old institutions from the Database.
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


def update_institutions(conn, sqlite, finc, k10plus, ai):
    """
    Update the institution table.
    """
    current_institutions = get_all_current_institutions(conn, sqlite, finc, ai)
    old_institutions = get_all_old_institutions(conn, sqlite)

    # Check if the institution table is allready filled and this is not the first checkup
    if len(old_institutions) > 5:
        institution_table_is_filled = True
    else:
        institution_table_is_filled = False

    for old_institution in old_institutions:
        if institution_table_is_filled and old_institution not in current_institutions:
            print("The institution %s is no longer in Solr." % old_institution)
            print("Please delete it from the institution table if this change is permanent.")

    for current_institution in current_institutions:
        if current_institution not in old_institutions:
            print("The institution %s is new in Solr." % current_institution)
            sql = "INSERT INTO institution (institution) VALUES ('%s')" % current_institution
            sqlite.execute(sql)
            conn.commit()


def update_history(conn, sqlite, finc, k10plus, ai):
    """
    Get all current sources and title numbers from Solr and log them into database.
    """
    current_sources = get_all_current_sources(conn, sqlite, finc, ai)
    current_institutions = get_all_current_institutions(conn, sqlite, finc, ai)
    current_sourcebyinstitution = get_all_current_institutions(conn, sqlite, finc, ai)

    for source in current_sources:
        
        for institution in current_institutions:

            # check finc main
            sourcebyinstitution = str(source) + " - " + institution
            resp = requests.get("http://" + finc + '/solr/biblio/select?q=source_id%3A' + str(source) + '+AND+institution%3A"' + institution + '"&rows=0&wt=json&indent=true')
            resp = resp.json()  
            number = resp["response"]["numFound"]
            if number != 0:
                sql = 'INSERT INTO log (sourcebyinstitution, titles) VALUES ("%s", %s)' % (sourcebyinstitution, number)
                sqlite.execute(sql)
                conn.commit()
            else:
                # check ai
                resp = requests.get("http://" + ai + '/solr/biblio/select?q=source_id%3A' + str(source) + '+AND+institution%3A"' + institution + '"&rows=0&wt=json&indent=true')
                resp = resp.json()
                number = resp["response"]["numFound"]
                if number != 0:
                    sql = 'INSERT INTO log (sourcebyinstitution, titles) VALUES ("%s", %s)' % (sourcebyinstitution, number)
                    sqlite.execute(sql)
                    conn.commit()

            # requests.exceptions.ConnectionError: HTTPConnectionPool(XXXXXX): Max retries exceeded
            time.sleep(0.25)


# Parse keyword arguments
parser = argparse.ArgumentParser()
parser.add_argument("-v",
                    action="version",
                    help="show version",
                    version="0.0.1")
parser.add_argument("-d",
                    dest="database",
                    help="path to database",
                    metavar="database")
parser.add_argument("-y",
                    dest="yaml",
                    help="link to review.yaml or another yaml template",
                    metavar="yaml")
parser.add_argument("-t",
                    dest="token",
                    help="private token for GitLab",
                    metavar="token")
parser.add_argument("-f",
                    dest="finc",
                    help="url of the finc main index",
                    metavar="finc")
parser.add_argument("-k",
                    dest="k10plus",
                    help="url of the k10plus index",
                    metavar="k10plus")
parser.add_argument("-a",
                    dest="ai",
                    help="url of the ai index",
                    metavar="ai")

args = parser.parse_args()

# Set default path if no database was specified
database = args.database
if not database:
    database = "/tmp/solrcheckup.sqlite"

# Exit when using yaml template without private token
yaml = args.yaml
token = args.token
if yaml and not token:
    sys.exit("Keyword argument for private token needed when using yaml template.") 

# Ensure that all three indicies are specified
finc = args.finc
k10plus = args.k10plus
ai = args.ai
if not finc or not k10plus or not ai:
    sys.exit("Three keyword arguments needed for finc, k10plus and ai index.")

# Check if database already exists, otherwise create new one
if not os.path.isfile(database):

    conn, sqlite = create_connection_and_set_cursor(database)

    sql = """
            CREATE TABLE
                source
                    (source INT PRIMARY KEY NOT NULL,
                    collection VARCHAR(30),
                    mega_collection VARCHAR(50))
        """

    sqlite.execute(sql)

    sql = """
            CREATE TABLE
                institution
                    (institution VARCHAR(30) PRIMARY KEY NOT NULL)                   
        """

    sqlite.execute(sql)

    sql = """
            CREATE TABLE
                sourcebyinstitution
                    (sourcebyinstitution VARCHAR(30) PRIMARY KEY NOT NULL)
        """

    sqlite.execute(sql)

    sql = """
            CREATE TABLE
                log
                    (date DEFAULT CURRENT_TIMESTAMP,
                    sourcebyinstitution VARCHAR(30) NOT NULL,
                    titles INT NOT NULL)
        """

    sqlite.execute(sql)

else:
    conn, sqlite = create_connection_and_set_cursor(database)

# 1. Step: Update the source table
update_sources(conn, sqlite, finc, k10plus, ai)

# 2. Step: Update the institution table 
update_institutions(conn, sqlite, finc, k10plus, ai)

# 3. Step: Get the number of titles for each SID and log them to database
update_history(conn, sqlite, finc, k10plus, ai)


sqlite.close()
