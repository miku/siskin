# coding: utf-8
# pylint: disable=C0301,E1101,C0103

# Copyright 2017 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
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

#
# BTAG preparation
# ================

import collections
import json
import random

import luigi

from gluish.format import TSV
from siskin.database import sqlitedb
from siskin.sources.crossref import CrossrefUniqItems
from siskin.sources.doaj import DOAJDOIList, DOAJDump
from siskin.sources.mag import MAGReferenceDB
from siskin.task import DefaultTask


class BTAGTask(DefaultTask):
    """ Base task for BTAG. """
    TAG = 'btag'


class BTAGCrossrefSet(BTAGTask):
    """
    Find a set of crossref records with duplicates and references.
    """

    max = luigi.IntParameter(default=100000000)
    minrefs = luigi.IntParameter(default=2)

    def requires(self):
        return {
            'crossref': CrossrefUniqItems(),
            'doaj': DOAJDOIList(),
            'magdb': MAGReferenceDB()
        }

    def run(self):
        """
        Write out the doi, the number of refs found in MAG and whether the doi is in DOAJ as well.
        """
        doaj = set()
        with self.input().get('doaj').open() as handle:
            for line in handle:
                doaj.add(line.strip())

        with sqlitedb(self.input().get('magdb').path) as cursor:
            with self.input().get('crossref').open() as handle:
                with self.output().open('w') as output:
                    stmt = """
                    select doi from lookup where id IN (
                        select ref from refs where id = (
                        select id from lookup where doi = ?))
                    """
                    for i, line in enumerate(handle):
                        if i > self.max:
                            break
                        doc = json.loads(line)
                        doi = doc.get('DOI')
                        if doi is not None:
                            cursor.execute(stmt, (doi,))
                            rows = cursor.fetchall()
                            refs = [v[0] for v in rows]
                            if len(refs) >= self.minrefs:
                                output.write_tsv(doi, len(refs), doi in doaj)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class BTAGCrossref(BTAGTask):
    """
    Create a crossref subset, that contains items, that have references in MAG and dups in DOAJ.
    """

    def requires(self):
        return {
            'crossref': CrossrefUniqItems(),
            'set': BTAGCrossrefSet(max=5000000),
        }

    def run(self):
        entries = {}
        with self.input().get('set').open() as handle:
            for row in handle.iter_tsv(cols=('doi', 'refcount', 'indoaj')):
                entries[row.doi] = row

        with self.input().get('crossref').open() as handle:
            with self.output().open('w') as output:
                for line in handle:
                    doc = json.loads(line)
                    if doc.get("DOI") in entries:
                        output.write(json.dumps(doc) + "\n")
                    else:
                        if random.random() < 0.0001:
                            self.logger.debug("inlude random record")
                            output.write(json.dumps(doc) + "\n")

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))


class BTAGDOAJSubset(BTAGTask):
    """
    A smaller subset of DOAJ.
    """

    def requires(self):
        return {
            'doaj': DOAJDump(),
            'set': BTAGCrossrefSet(max=5000000),
        }

    def run(self):
        entries = {}
        with self.input().get('set').open() as handle:
            for row in handle.iter_tsv(cols=('doi', 'refcount', 'indoaj')):
                entries[row.doi] = row

        with self.input().get('doaj').open() as handle:
            with self.output().open('w') as output:
                for line in handle:
                    doc = json.loads(line)
                    for iddoc in doc['_source']['bibjson']['identifier']:
                        if iddoc["type"] != "doi":
                            continue
                        doi = iddoc["id"]
                        if doi in entries:
                            output.write(json.dumps(doc) + "\n")
                        else:
                            if random.random() < 0.001:
                                self.logger.debug("inlude random record")
                                output.write(json.dumps(doc) + "\n")

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))


class BTAGMAGSubset(BTAGTask):
    """ A small subset of MAG. """

    def requires(self):
        return {
            'db': MAGReferenceDB(),
            'crossref': BTAGCrossref(),
        }

    def run(self):
        doiset = set()
        with self.input().get('crossref').open() as handle:
            for line in handle:
                doc = json.loads(line)
                doiset.add(doc['DOI'])

        with sqlitedb(self.input().get('db').path) as cursor:
            with self.output().open('w') as output:
                stmt = """
                    select doi from lookup where id IN (
                    select ref from refs where id = (
                        select id from lookup where doi = ?))
                """
                for doi in doiset:
                    cursor.execute(stmt, (doi,))
                    rows = cursor.fetchall()
                    refs = [v[0] for v in rows]
                    output.write_tsv(doi, *refs)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'), format=TSV)
