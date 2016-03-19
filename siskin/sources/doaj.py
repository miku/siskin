# coding: utf-8
# pylint: disable=F0401,W0232,E1101,C0103,C0301,W0223,E1123,R0904,E1103

#  Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                    The Finc Authors, http://finc.info
#                    Martin Czygan, <martin.czygan@uni-leipzig.de>
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
Directory of Open Access Journals.

DOAJ is an online directory that indexes and provides access to high quality,
open access, peer-reviewed journals.

http://doaj.org
"""

from siskin.benchmark import timed
from gluish.common import Executable
from siskin.database import sqlitedb
from gluish.format import TSV
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.sources.bsz import FincMappingDump
from siskin.task import DefaultTask
from siskin.utils import ElasticsearchMixin
import datetime
import elasticsearch
import json
import luigi
import tempfile
import time

class DOAJTask(DefaultTask):
    """
    Base task for DOAJ.
    """
    TAG = '028'

    def closest(self):
        return monthly(date=self.date)

class DOAJCSV(DOAJTask):
    """
    CSV dump, updated every 30 minutes. Not sure what's in there.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    url = luigi.Parameter(default='http://doaj.org/csv', significant=False)

    def requires(self):
        return Executable(name='wget', message='http://www.gnu.org/software/wget/')

    @timed
    def run(self):
        output = shellout('wget --retry-connrefused {url} -O {output}', url=self.url)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='csv'))

class DOAJDump(DOAJTask):
    """
    Complete DOAJ Elasticsearch dump., refs: #2089.
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
        """
        Connect to ES and issue queries. Use exponential backoff to mitigate
        gateway timeouts. Be light on resources and do not crawl in parallel.
        """
        max_backoff_retry = 10
        backoff_interval_s = 0.05

        hosts = [{'host': self.host, 'port': self.port, 'url_prefix': self.url_prefix}]
        es = elasticsearch.Elasticsearch(hosts, timeout=self.timeout, max_retries=self.max_retries, use_ssl=True)
        with self.output().open('w') as output:
            offset, total = 0, 0
            while offset <= total:
                for i in range(1, max_backoff_retry + 1):
                    self.logger.debug(json.dumps({'attempt': i, 'offset': offset, 'total': total}))

                    try:
                        result = es.search(body={'constant_score': {'query': {'match_all': {}}}},
                                           index=('journal', 'article'),
                                           size=self.batch_size, from_=offset)
                    except Exception:
                        if i == max_backoff_retry:
                            raise
                        time.sleep(backoff_interval_s)
                        backoff_interval_s = 2 * backoff_interval_s
                        continue

                    for doc in result['hits']['hits']:
                        output.write("%s\n" % json.dumps(doc))
                    total = total or result['hits']['total']
                    offset += self.batch_size
                    break

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class DOAJFiltered(DOAJDump):
    """
    Filter DOAJ by ISSN in assets. Slow.
    """
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
    """
    An indexable JSON.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'input': DOAJFiltered(date=self.date),
                'jq': Executable(name='jq', message='http://git.io/NYpfTw')}

    @timed
    def run(self):
        output = shellout("jq -r -c '._source' {input} > {output}", input=self.input().get('input').path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class DOAJIndex(DOAJTask, ElasticsearchMixin):
    """
    Index into Elasticsearch.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    index = luigi.Parameter(default='doaj')

    def requires(self):
        return {'input': DOAJJson(date=self.date),
                'esbulk': Executable(name='esbulk', message='http://git.io/vY5ny'),
                'curl': Executable(name='curl', message='http://curl.haxx.se/')}

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
        shellout("esbulk -verbose -index {index} {input}", index=self.index, input=self.input().get('input').path)
        with self.output().open('w'):
            pass

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class DOAJIntermediateSchema(DOAJTask):
    """
    Convert to intermediate schema via span.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'span-import': Executable(name='span-import', message='http://git.io/vI8NV'),
                'input': DOAJFiltered(date=self.date)}

    @timed
    def run(self):
        output = shellout("span-import -i doaj {input} | pigz -c > {output}", input=self.input().get('input').path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))

class DOAJISSNList(DOAJTask):
    """
    A list of DOAJ ISSNs.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'input': DOAJIntermediateSchema(date=self.date),
                'jq': Executable(name='jq', message='http://git.io/NYpfTw')}

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout("""jq -r '.["rft.issn"][]?' <(unpigz -c {input}) >> {output} """, input=self.input().get('input').path, output=stopover)
        shellout("""jq -r '.["rft.eissn"][]?' <(unpigz -c {input}) >> {output} """, input=self.input().get('input').path, output=stopover)
        output = shellout("""sort -u {input} > {output} """, input=stopover)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class DOAJDOIList(DOAJTask):
    """
    An best-effort list of DOAJ DOIs.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'input': DOAJIntermediateSchema(date=self.date),
                'jq': Executable(name='jq', message='http://git.io/NYpfTw')}

    @timed
    def run(self):
        output = shellout("""jq -r '.doi' <(unpigz -c {input}) | grep -v "null" | grep -o "10.*" 2> /dev/null | sort -u > {output} """,
                          input=self.input().get('input').path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class DOAJFincIDAlignment(DOAJTask):
    """
    FincID alignment, refs: #4494.
    """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return FincMappingDump(date=self.closest())

    def run(self):
        with sqlitedb(self.input().path) as conn:
            with self.output().open('w') as output:
                conn.execute("""SELECT finc_id, record_id FROM finc_mapping WHERE source_id = ?""", ('28',))
                for row in conn.fetchall():
                    output.write_tsv(row[0], 'ai-28-%s' % row[1])

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
