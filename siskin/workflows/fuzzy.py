# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

"""
Fuzzy deduplication related tasks.
"""

from elasticsearch import helpers as eshelpers
from gluish.benchmark import timed
from gluish.common import ElasticsearchMixin, Executable
from gluish.format import TSV
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
import datetime
import elasticsearch
import luigi
import marcx
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
    date = ClosestDateParameter(default=datetime.date.today())
    index = luigi.Parameter(description='name of the index')

    @timed
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
    date = ClosestDateParameter(default=datetime.date.today())
    indices = luigi.Parameter(default="bsz ebl nep")

    def requires(self):
        indices = self.indices.split()
        for index in indices:
            yield FuzzyEditionList(date=self.date, index=index,
                                   es_host=self.es_host, es_port=self.es_port)

    @timed
    def run(self):
        _, output = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path, output=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True), format=TSV)

class FuzzyFieldList(FuzzyTask, ElasticsearchMixin):
    """ Run estab to gather a TSV of (id, index, type, values...) for  comparisons. """
    date = ClosestDateParameter(default=datetime.date.today())
    indices = luigi.Parameter(description='indices to extract values from')
    fields = luigi.Parameter(default='_id _index _type content.245.a content.245.b', description='values to extract')

    def requires(self):
        return Executable(name='estab', message='http://git.io/bLY7cQ')

    def run(self):
        output = shellout("""estab -host {host} -port {port} -indices "{indices}" -f "{fields}" > {output} """,
                          indices=self.indices,fields=self.fields, host=self.es_host, port=self.es_port)
        luigi.File(output).move(self.output().path)

    def output(self):
            return luigi.LocalTarget(path=self.path(digest=True), format=TSV)

class FuzzyDeduplication(FuzzyTask, ElasticsearchMixin):
    """ Deduplicate items. """

    date = ClosestDateParameter(default=datetime.date.today())
    source = luigi.Parameter(description='indices that are compared')
    target = luigi.Parameter(description='indices that are compared against')

    def requires(self):
        return {'file': FuzzyFieldList(date=self.date, indices=self.source, fields='_id _index _type content.245.a content.245.b'),
                'esmlt': Executable(name='esmlt', message='http://git.io/ckXUgA')}

    def run(self):
        # find similar titles
        output = shellout("""esmlt -indices "{target}" -fields "content.245.a content.245.b"
                             -file "{file}" -columns "4,5" > {output} """, file=self.input().get('file').path, target=self.target)
        # TODO: postprocessing similarity measures
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True), format=TSV)
