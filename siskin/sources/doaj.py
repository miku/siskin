# coding: utf-8
# pylint: disable=F0401,W0232,E1101,C0103,C0301
"""
Directory of Open Access Journals.
"""

from gluish.benchmark import timed
from gluish.common import Executable
from gluish.database import sqlite3db
from gluish.format import TSV
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.configuration import Config
from siskin.sources.bsz import FincMappingDump
from siskin.task import DefaultTask
from siskin.utils import ElasticsearchMixin
import datetime
import elasticsearch
import json
import luigi
import tempfile

config = Config.instance()

class DOAJTask(DefaultTask):
    """ Base task for DOAJ. """
    TAG = '028'

    def closest(self):
        """ Monthly schedule. """
        return monthly(date=self.date)

class DOAJCSV(DOAJTask):
    """ CSV dump, updated every 30 minutes. Not sure what's in there. """
    date = luigi.DateParameter(default=datetime.date.today())
    url = luigi.Parameter(default='http://doaj.org/csv', significant=False)

    @timed
    def run(self):
        """ Just download file. """
        output = shellout('wget --retry-connrefused {url} -O {output}', url=self.url)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='csv'))

class DOAJChunk(DOAJTask):
    """
    Retrieve a chunk of data from DOAJ. Mitigates HTTP 502 and 504 errors from DOAJ.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    offset = luigi.IntParameter(default=0)
    size = luigi.IntParameter(default=100000, description="must be a multiple of batch_size")

    host = luigi.Parameter(default='doaj.org', significant=False)
    port = luigi.IntParameter(default=443, significant=False)
    url_prefix = luigi.Parameter(default='query', significant=False)

    batch_size = luigi.IntParameter(default=500, significant=False)
    timeout = luigi.IntParameter(default=120, significant=False)
    max_retries = luigi.IntParameter(default=10, significant=False)

    @timed
    def run(self):
        """ Connect to ES and issue queries. TODO: See if they support scan. """
        hosts = [{'host': self.host, 'port': self.port, 'url_prefix': self.url_prefix}]
        es = elasticsearch.Elasticsearch(hosts, timeout=self.timeout, max_retries=self.max_retries, use_ssl=True)
        with self.output().open('w') as output:
            offset, total = self.offset, self.offset + self.size
            while offset <= total:
                self.logger.debug(json.dumps({'offset': offset, 'total': total}))
                result = es.search(body={'constant_score':
                                   {'query': {'match_all': {}}}},
                                   index=('journal', 'article'),
                                   size=self.batch_size, from_=offset)
                if len(result['hits']['hits']) == 0:
                    break
                for doc in result['hits']['hits']:
                    output.write("%s\n" % json.dumps(doc))
                total = total or result['hits']['total']
                offset += self.batch_size

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class DOAJChunkedDump(DOAJTask):
    """ Retrieve DOAJ in chunks. """
    date = ClosestDateParameter(default=datetime.date.today())

    host = luigi.Parameter(default='doaj.org', significant=False)
    port = luigi.IntParameter(default=443, significant=False)
    url_prefix = luigi.Parameter(default='query', significant=False)

    batch_size = luigi.IntParameter(default=1000, significant=False)
    timeout = luigi.IntParameter(default=60, significant=False)
    max_retries = luigi.IntParameter(default=3, significant=False)

    size = luigi.IntParameter(default=100000, significant=False)

    def requires(self):
        hosts = [{'host': self.host, 'port': self.port, 'url_prefix': self.url_prefix}]
        es = elasticsearch.Elasticsearch(hosts, timeout=self.timeout, max_retries=self.max_retries, use_ssl=True)
        result = es.search(body={'constant_score':
                           {'query': {'match_all': {}}}},
                           index=('journal', 'article'), size=1, from_=0)
        offset, total = 0, result['hits']['total']
        while offset < total:
            yield DOAJChunk(host=self.host, port=self.port, url_prefix=self.url_prefix, batch_size=self.batch_size,
                            timeout=self.timeout, max_retries=self.max_retries, offset=offset, size=self.size)
            offset += self.size

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class DOAJDump(DOAJTask):
    """
    Complete DOAJ Elasticsearch dump. For a slightly filtered version see: DOAJFiltered.
    Deprecated: One task to download 8G is more fragile, that 8 tasks downloading 1G each.
    See: DOAJChunkedDump.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    host = luigi.Parameter(default='doaj.org', significant=False)
    port = luigi.IntParameter(default=443, significant=False)
    url_prefix = luigi.Parameter(default='query', significant=False)

    batch_size = luigi.IntParameter(default=1000, significant=False)
    timeout = luigi.IntParameter(default=60, significant=False)
    max_retries = luigi.IntParameter(default=3, significant=False)

    @timed
    def run(self):
        """ Connect to ES and issue queries. TODO: See if they support scan. """
        hosts = [{'host': self.host, 'port': self.port, 'url_prefix': self.url_prefix}]
        es = elasticsearch.Elasticsearch(hosts, timeout=self.timeout, max_retries=self.max_retries, use_ssl=True)
        with self.output().open('w') as output:
            offset, total = 0, 0
            while offset <= total:
                self.logger.debug(json.dumps({'offset': offset, 'total': total}))
                result = es.search(body={'constant_score':
                                   {'query': {'match_all': {}}}},
                                   index=('journal', 'article'),
                                   size=self.batch_size, from_=offset)
                for doc in result['hits']['hits']:
                    output.write("%s\n" % json.dumps(doc))
                total = total or result['hits']['total']
                offset += self.batch_size

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class DOAJFiltered(DOAJDump):
    """ Filter DOAJ by ISSN in assets. Slow. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return DOAJDump(date=self.date)

    @timed
    def run(self):
        excludes = set()
        with open(self.assets('028_doaj_filter.tsv')) as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                excludes.add(line.replace("-", ""))

        with self.output().open('w') as output:
            with self.input().open() as handle:
                for line in handle:
                    record, skip = json.loads(line), False
                    for issn in record["_source"]["index"]["issn"]:
                        issn = issn.replace("-", "").strip()
                        if issn in excludes:
                            skip = True
                            break
                    if skip:
                        continue
                    output.write(line)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class DOAJJson(DOAJTask):
    """ An indexable JSON. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
       return DOAJFiltered(date=self.date)

    @timed
    def run(self):
        output = shellout("jq -r -c '._source' {input} > {output}", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class DOAJIndex(DOAJTask, ElasticsearchMixin):
    """ Index. """
    date = ClosestDateParameter(default=datetime.date.today())
    index = luigi.Parameter(default='doaj')

    def requires(self):
       return DOAJJson(date=self.date)

    @timed
    def run(self):
        es = elasticsearch.Elasticsearch()
        shellout("curl -XDELETE {host}:{port}/{index}", host=self.es_host, port=self.es_port, index=self.index)
        mapping = {
            'default': {
                'date_detection': False,
                '_id': {
                    'path': 'URL'
                },
            }
        }
        es.indices.create(index=self.index)
        es.indices.put_mapping(index='bsz', doc_type='default', body=mapping)
        shellout("esbulk -verbose -index {index} {input}", index=self.index, input=self.input().path)
        with self.output().open('w'):
            pass

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class DOAJIntermediateSchema(DOAJTask):
    """ Convert to intermediate format via span. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
       return {'span': Executable(name='span-import', message='http://git.io/vI8NV'),
               'file': DOAJFiltered(date=self.date)}

    @timed
    def run(self):
        output = shellout("span-import -i doaj {input} > {output}", input=self.input().get('file').path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class DOAJISSNList(DOAJTask):
    """ A list of JSTOR ISSNs. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return DOAJIntermediateSchema(date=self.date)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout("""jq -r '.["rft.issn"][]' {input} 2> /dev/null >> {output} """, input=self.input().path, output=stopover)
        shellout("""jq -r '.["rft.eissn"][]' {input} 2> /dev/null >> {output} """, input=self.input().path, output=stopover)
        output = shellout("""sort -u {input} > {output} """, input=stopover)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class DOAJDOIList(DOAJTask):
    """ A list of JSTOR DOIs. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return DOAJIntermediateSchema(date=self.date)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout("""jq -r '.doi' {input} | grep -v "null" | grep -o "10.*" 2> /dev/null > {output} """, input=self.input().path, output=stopover)
        output = shellout("""sort -u {input} > {output} """, input=stopover)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class DOAJRaw(DOAJTask):
    """ Strip index information from dump. """
    date = ClosestDateParameter(default=datetime.date.today())

    host = luigi.Parameter(default='doaj.org', significant=False)
    port = luigi.IntParameter(default=80, significant=False)
    url_prefix = luigi.Parameter(default='query', significant=False)

    def requires(self):
        return DOAJFiltered()

    def run(self):
        output = shellout("jq -M -c '._source' {input} > {output}", input=self.input().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class DOAJFincIDAlignment(DOAJTask):
    """ FincID alignment. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return FincMappingDump(date=self.closest())

    def run(self):
        with sqlite3db(self.input().path) as conn:
            with self.output().open('w') as output:
                conn.execute("""SELECT finc_id, record_id FROM finc_mapping WHERE source_id = ?""", ('28',))
                for row in conn.fetchall():
                    output.write_tsv(row[0], 'ai-28-%s' % row[1])

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
