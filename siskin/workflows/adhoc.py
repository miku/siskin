# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

"""
A place for one time tasks and quick write-ups.
"""

from elasticsearch import helpers as eshelpers
from gluish.format import TSV
from gluish.utils import itervalues
from siskin.task import DefaultTask
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
