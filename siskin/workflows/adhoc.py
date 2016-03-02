# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301
#
#  Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                 by The Finc Authors, http://finc.info
#                 by Martin Czygan, <martin.czygan@uni-leipzig.de>
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

"""
A place for one time tasks and quick write-ups.
"""

from elasticsearch import helpers as eshelpers
from gluish.format import TSV
from siskin.sources.crossref import CrossrefDOIAndISSNList
from siskin.task import DefaultTask
import csv
import datetime
import elasticsearch
import json
import luigi
import marcx
import re

class AdhocTask(DefaultTask):
    """ Tasks that do not fit elsewhere. """
    TAG = 'adhoc'

class DOIList(AdhocTask):
    """
    For all open indices, run a heuristic DOI query and dump a TSV of
    (index, id, DOI, year, title and subtitle) tuples.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    encoding = luigi.Parameter(default='utf-8', significant=False)
    na = luigi.Parameter(default='NOT_AVAILABLE', significant=False)
    scroll = luigi.Parameter(default='10m', significant=False)
    timeout = luigi.IntParameter(default=30, significant=False)

    def run(self):
        es = elasticsearch.Elasticsearch(timeout=self.timeout)
        hits = eshelpers.scan(es, {'query': {"regexp": {"_all": "10\\.[0-9]{4,}"}}}, scroll=self.scroll)
        with self.output().open('w') as output:
            for hit in hits:
                doc_id, index = hit.get('_id'), hit.get('_index')
                content = hit.get('_source').get('content')
                matches = re.findall(r"10\.[0-9]{4,}/[^ \t\"]{3,}", json.dumps(content))
                for candidate in set(matches):
                    doc = marcx.marcdoc(hit)
                    title = (doc.values('245.a') or [self.na])[0]
                    subtitle = (doc.values('245.b') or [self.na])[0]
                    pubyear = (doc.values('260.c') or [self.na])[0]
                    output.write_tsv(index,
                                     doc_id,
                                     candidate.encode(self.encoding),
                                     pubyear.encode(self.encoding),
                                     title.encode(self.encoding),
                                     subtitle.encode(self.encoding))

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class CrossrefWebOfScienceMatch(AdhocTask):
    """
    Find all ISSNs for Web of Science export.

    The input --file must contain one DOI per line:

    005410.001002/14651858.CD14005465.pub14651852
    10.1002/ange.201106065
    10.1007/BFb0061839.56
    ..
    """
    date = luigi.DateParameter(default=datetime.date.today())
    file = luigi.Parameter(description='web of science doi list', significant=False)

    def requires(self):
        return CrossrefDOIAndISSNList(date=self.date)

    def run(self):
        # wos contains all WOS DOIs, found keeps the DOIs, that could be found in Crossref
        wos, found = set(), set()

        with open(self.file) as handle:
            for line in handle:
                wos.add(line.strip().lower())

        with self.input().open() as handle:
            with self.output().open('w') as output:
                for line in handle:
                    parts = [s.strip('"') for s in line.strip().split(',')]
                    doi, issns = parts[0].lower(), parts[1:]
                    if doi in wos:
                        output.write_tsv('OK', doi, *issns)
                        found.add(doi)

                for id in wos.difference(found):
                    output.write_tsv('NOT_FOUND', id)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
