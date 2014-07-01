# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

"""
Fuzzy deduplication related tasks.
"""

from elasticsearch import helpers as eshelpers
from gluish.common import ElasticsearchMixin
from gluish.format import TSV
from gluish.intervals import weekly
from gluish.utils import shellout
from siskin.task import DefaultTask
import datetime
import elasticsearch
import luigi
import tempfile

class FuzzyTask(DefaultTask):
    """ Fuzzy deduplication related things. """
    TAG = 'fuzzy'

    def closest(self):
        return weekly(self.date)

class FuzzyEditionList(FuzzyTask, ElasticsearchMixin):
    """ For a single index, create a TSV of the form

        Index, Id, Edition Statement

    where Edition Statement (250.a) is either NULL, a single string of multiple
    strings, separeted by a pipe (|).

    Index parameters can be adjusted like (space separated values in a single string):

        $ taskdo FuzzyEditionList --index ebl
    """
    date = luigi.DateParameter(default=datetime.date.today())
    index = luigi.Parameter(description='name of the index')

    def run(self):
        es = elasticsearch.Elasticsearch([dict(host=self.es_host, port=self.es_port)])
        hits = eshelpers.scan(es, {'query': {'match_all': {}},
            'fields': ["content.250.a"]}, index=self.index,
            doc_type='title', scroll='10m', size=10000)
        with self.output().open('w') as output:
            for hit in hits:
                fields = hit.get('fields')
                if fields:
                    edition = 'NULL'
                    entries = fields.get('content.250.a', [])
                    if entries:
                        edition = '|'.join(entries).encode('utf-8')
                    output.write_tsv(hit['_id'], hit['_index'], edition)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class FuzzyEditionListRange(FuzzyTask, ElasticsearchMixin):
    """
    For a list of indices, create a FuzzyEditionList and concatenate them.

        $ taskdo FuzzyEditionList --indices "bsz ebl nep"
    """
    date = luigi.DateParameter(default=datetime.date.today())
    indices = luigi.Parameter(default="bsz ebl nep")

    def requires(self):
        indices = self.indices.split()
        for index in indices:
            yield FuzzyEditionList(date=self.date, index=index,
                                   es_host=self.es_host, es_port=self.es_port)

    def run(self):
        _, output = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path, output=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True), format=TSV)
