# coding: utf-8
# pylint: disable=F0401,W0232,E1101,C0103,C0301
"""
Directory of Open Access Journals.
"""

from gluish.benchmark import timed
from gluish.common import Executable
from gluish.format import TSV
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.configuration import Config
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

class DOAJDump(DOAJTask):
    """ Complete DOAJ Elasticsearch dump. """
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

class DOAJJson(DOAJTask):
    """ An indexable JSON. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
       return DOAJDump(date=self.date)

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
               'file': DOAJDump(date=self.date)}

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
        return DOAJDump()

    def run(self):
        output = shellout("jq -M -c '._source' {input} > {output}", input=self.input().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))
