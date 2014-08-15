# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

"""
Fuzzy deduplication related tasks.
"""

from elasticsearch import helpers as eshelpers
from gluish.benchmark import timed
from gluish.common import ElasticsearchMixin
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

class FuzzySimilarItems(FuzzyTask, ElasticsearchMixin):
    """
    For a single file containing (index, id) tuples and a list of indices
    report all records in the indices, that are similar to those given in the file.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    filename = luigi.Parameter(description="path to a TSV (Index, ID)")
    indices = luigi.Parameter(default="bsz ebl nep")

    max_query_terms = luigi.IntParameter(default=25)
    min_term_freq = luigi.IntParameter(default=1)
    size = luigi.IntParameter(default=5, description="number of similar items to return")

    def run(self):
	"""
	245.{a,b,c} are NR / http://www.loc.gov/marc/bibliographic/bd245.html
	"""
	indices = self.indices.split()
	es = elasticsearch.Elasticsearch([dict(host=self.es_host, port=self.es_port)])
	with luigi.File(self.filename, format=TSV).open() as handle:
	    with self.output().open('w') as output:
		for row in handle.iter_tsv(cols=('index', 'id')):
		    try:
			doc = marcx.DotDict(es.get_source(index=row.index, id=row.id))
			if not doc['content'].get('245'):
			    continue
			title = doc.content['245'][0].get('a', [''])[0]
			subtitle = doc.content['245'][0].get('b', [''])[0]
			like_text = '%s %s' % (title, subtitle)

			query = {'query': {
			    'more_like_this': {
				'fields': ['content.245.a', 'content.245.b'],
				'like_text': like_text,
				'stop_words': [],
				'min_term_freq' : 1,
				'max_query_terms' : 25,
			    }
			}}

			response = es.search(index=indices, body=query, size=self.size)
			hits = response['hits']['hits']
			for hit in hits:
			    output.write_tsv(row.index, row.id, hit['_index'], hit['_id'])

		    except elasticsearch.exceptions.TransportError as err:
			self.logger.warn(err)
			continue

    def output(self):
	return luigi.LocalTarget(path=self.path(digest=True), format=TSV)
