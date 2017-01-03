# coding: utf-8
# pylint: disable=C0301,C0330,W0622,E1101

# Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
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

"""

CrossRef is an association of scholarly publishers
that develops shared infrastructure to support
more effective scholarly communications.

Our citation-linking network today covers over 68
million journal articles and other content items
(books chapters, data, theses, technical reports)
from thousands of scholarly and professional
publishers around the globe.

Configuration
-------------

[crossref]

doi-blacklist = /tmp/siskin-data/crossref/CrossrefDOIBlacklist/output.tsv

"""

import datetime
import json
import os
import tempfile
import time
import urllib

import luigi
import requests

import elasticsearch
from gluish.common import Executable
from gluish.format import TSV, Gzip
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import date_range, shellout
from siskin.benchmark import timed
from siskin.sources.amsl import AMSLFilterConfig, AMSLService
from siskin.task import DefaultTask
from siskin.utils import ElasticsearchMixin, URLCache


class CrossrefTask(DefaultTask):
    """
    Crossref related tasks. See: http://www.crossref.org/
    """
    TAG = 'crossref'

    def closest(self):
        """
        Update frequency.
        """
        return monthly(date=self.date)


class CrossrefHarvestChunkWithCursor(CrossrefTask):
    """
    Task should have the same output as `CrossrefHarvestChunk`, but
    implementation should use cursor (https://git.io/v1K27).
    """
    begin = luigi.DateParameter()
    end = luigi.DateParameter()
    filter = luigi.Parameter(default='deposit', description='index, deposit, update')

    rows = luigi.IntParameter(default=1000, significant=False)
    max_retries = luigi.IntParameter(default=10, significant=False, description='HTTP retries')
    attempts = luigi.IntParameter(default=3, significant=False, description='number of attempts to GET an URL that failed')
    sleep = luigi.IntParameter(default=1, significant=False, description='sleep between requests')

    def run(self):
        """
        > Using large offset values can result in extremely long response times. Offsets
        in the 100,000s and beyond will likely cause a timeout before the API is able
        to respond. An alternative to paging through very large result sets (like a
        corpus used for text and data mining) it to use the API's exposure of Solr's
        deep paging cursors. Any combination of query, filters and facets may be used
        with deep paging cursors. While rows may be specified along with cursor,
        offset and sample cannot be used. To use deep paging make a query as normal,
        but include the cursor parameter with a value of *
        """
        cache = URLCache(directory=os.path.join(tempfile.gettempdir(), '.urlcache'))
        adapter = requests.adapters.HTTPAdapter(max_retries=self.max_retries)
        cache.sess.mount('http://', adapter)

        filter = "from-{self.filter}-date:{self.begin},until-{self.filter}-date:{self.end}".format(self=self)
        rows, offset = self.rows, 0

        cursor = '*'

        with self.output().open('w') as output:
            while True:
                params = {
                    'rows': rows,
                    'filter': filter,
                    'cursor': cursor
                }

                url = 'http://api.crossref.org/works?%s' % (urllib.urlencode(params))

                for attempt in range(1, self.attempts):
                    if not cache.is_cached(url):
                        time.sleep(self.sleep)
                    body = cache.get(url)
                    try:
                        content = json.loads(body)
                    except ValueError as err:
                        if attempt == self.attempts - 1:
                            self.logger.debug('URL was %s', url)
                            self.logger.debug(err)
                            self.logger.debug(body[:100] + '...')
                            raise

                        cache_file = cache.get_cache_file(url)
                        if os.path.exists(cache_file):
                            self.logger.debug('trying to recover by removing cached entry at %s', cache_file)
                            os.remove(cache_file)
                    else:
                        break  # all fine

                count = len(content['message']['items'])
                self.logger.debug("%s: %s", url, count)
                if count == 0:
                    break

                output.write(body + '\n')
                offset += rows

                if not 'next-cursor' in content['message']:
                    raise RuntimeError('expected next-cursor in message, but missing')
                cursor = content['message']['next-cursor']

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))


class CrossrefHarvestChunk(CrossrefTask):
    """
    >>> DEPRECATED, switch to CrossrefHarvestChunkWithCursor soon, (https://git.io/v1K27).

    Harvest a slice of Crossref.

    API docs can be found under: http://api.crossref.org/

    The output file is line delimited JSON, just the concatenated responses.
    """
    begin = luigi.DateParameter()
    end = luigi.DateParameter()
    filter = luigi.Parameter(default='deposit', description='index, deposit, update')

    rows = luigi.IntParameter(default=200, significant=False)
    max_retries = luigi.IntParameter(default=10, significant=False, description='HTTP retries')
    attempts = luigi.IntParameter(default=3, significant=False, description='number of attempts to GET an URL that failed')
    sleep = luigi.IntParameter(default=1, significant=False, description='sleep between requests (secs)')

    @timed
    def run(self):
        """
        The API sometimes returns a 504 or other error. We therefore cache all HTTP requests
        locally with a simple URLCache and re-attempt a URL a couple of times.
        """
        cache = URLCache(directory=os.path.join(tempfile.gettempdir(), '.urlcache'))
        adapter = requests.adapters.HTTPAdapter(max_retries=self.max_retries)
        cache.sess.mount('http://', adapter)

        filter = "from-{self.filter}-date:{self.begin},until-{self.filter}-date:{self.end}".format(self=self)
        rows, offset = self.rows, 0

        with self.output().open('w') as output:
            while True:
                params = {"rows": rows, "offset": offset, "filter": filter}
                url = 'http://api.crossref.org/works?%s' % (urllib.urlencode(params))
                for attempt in range(1, self.attempts):
                    if not cache.is_cached(url):
                        time.sleep(self.sleep)
                    body = cache.get(url)
                    try:
                        content = json.loads(body)
                    except ValueError as err:
                        if attempt == self.attempts - 1:
                            self.logger.debug("URL was %s", url)
                            self.logger.debug(err)
                            self.logger.debug(body[:100])
                            raise
                        if os.path.exists(cache.get_cache_file(url)):
                            self.logger.debug("trying to recover by removing cached entry")
                            os.remove(cache.get_cache_file(url))
                    else:
                        break
                items = content["message"]["items"]
                self.logger.debug("%s: %s", url, len(items))
                if len(items) == 0:
                    break
                output.write(body + "\n")
                offset += rows

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))


class CrossrefHarvest(luigi.WrapperTask, CrossrefTask):
    """
    Harvest everything in incremental steps. Yield the targets sorted by date (ascending).
    """
    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    end = luigi.DateParameter(default=datetime.date.today())
    update = luigi.Parameter(default='months', description='days, weeks or months')

    def requires(self):
        if self.update not in ('days', 'weeks', 'months'):
            raise RuntimeError('update can only be: days, weeks or months')
        dates = [dt for dt in date_range(self.begin, self.end, 1, self.update)]
        tasks = [CrossrefHarvestChunkWithCursor(begin=dates[i], end=dates[i + 1])
                 for i in range(len(dates) - 1)]
        return sorted(tasks)

    def output(self):
        return self.input()


class CrossrefChunkItems(CrossrefTask):
    """
    Extract the message items, per chunk.
    """
    begin = luigi.DateParameter()
    end = luigi.DateParameter()
    filter = luigi.Parameter(default='deposit', description='index, deposit, update')

    def requires(self):
        return CrossrefHarvestChunkWithCursor(begin=self.begin, end=self.end, filter=self.filter)

    def run(self):
        output = shellout("jq -c -r '.message.items[]?' {input} | pigz -c > {output}", input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)


class CrossrefLineDOI(CrossrefTask):
    """
    Extract the line number and DOI for a given chunk. Works per chunk, so updates can be more incremental.

    TODO: Sort by filename seems correct for now, even in some edge cases, like
    DOI 10.1177/0959353515613812:

    13046   ... CrossrefChunkItems/begin-2016-01-01-end-2016-02-01-filter-deposit.ldj.gz 10.1177/0959353515613812
    3548977 ... CrossrefChunkItems/begin-2016-01-01-end-2016-02-01-filter-deposit.ldj.gz 10.1177/0959353515613812

    Both docs come in a single API harvest slice, but they differ slightly:
    https://gist.github.com/miku/c9e892343d8b0acb49cf

    Document in line #3548977 would be the most recent and the current
    implementation find it:

        $ taskcat CrossrefDOITable | LC_ALL=C grep -F "10.1177/0959353515613812"
        3548977 ... CrossrefChunkItems/begin-2016-01-01-end-2016-02-01-filter-deposit.ldj.gz 10.1177/0959353515613812

    So by using tac we should get the correct answer here, but it might probably
    be more robust, to use `timestamp` or some inherent attribute of the record
    for ordering.

    Q: DOI "10.1016/j.visres.2013.12.006" is in CrossrefChunkItems, but does not
    get extracted here. Why?

    A: 10.1016/j.visres.2013.12.006 is only an associated DOI, here a
    "Corrigendum to" the original article, which has been published in a
    subsequent volume. """

    begin = luigi.DateParameter()
    end = luigi.DateParameter()
    filter = luigi.Parameter(default='deposit', description='index, deposit, update')

    def requires(self):
        return CrossrefChunkItems(begin=self.begin, end=self.end, filter=self.filter)

    def run(self):
        output = shellout(r"""jq -r '.DOI?' <(unpigz -c {input}) | awk '{{print NR"\t{input}\t"$0 }}' | pigz -c > {output}""", input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist.gz'))


class CrossrefLineDOIWrapper(CrossrefTask):
    """
    Run DOI and line extraction for each chunk.
    """
    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())
    update = luigi.Parameter(default='months', description='days, weeks or months')

    def requires(self):
        if self.update not in ('days', 'weeks', 'months'):
            raise RuntimeError('update can only be: days, weeks or months')
        dates = [dt for dt in date_range(self.begin, self.date, 1, self.update)]
        tasks = [CrossrefLineDOI(begin=dates[i - 1], end=dates[i]) for i in range(1, len(dates))]
        return sorted(tasks)

    def output(self):
        return self.input()


class CrossrefLineDOICombined(CrossrefTask):
    """
    Combine all extracted DOI and lines numbers from chunk files.
    Columns: filename lineno DOI.
    """
    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return CrossrefLineDOIWrapper(begin=self.begin, date=self.closest())

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path, output=stopover)
        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist.gz'))


class CrossrefDOITable(CrossrefTask):
    """
    Create a table of current crossref DOIs along with their origin. Output
    contains each DOI only once, accociated with the most recent version.
    """
    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return CrossrefLineDOICombined(begin=self.begin, date=self.closest())

    def run(self):
        output = shellout("""
            TMPDIR={tmpdir} LC_ALL=C sort -k2,2 -S25% <(TMPDIR={tmpdir} unpigz -c {input}) |
            TMPDIR={tmpdir} tac |
            TMPDIR={tmpdir} LC_ALL=C sort -S25% -u -k3,3 |
            pigz -c > {output}""", tmpdir=self.config.get('core', 'tempdir'), input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist.gz'), format=Gzip)


class CrossrefDOITableClean(CrossrefTask):
    """
    Clean the DOI table from DOIs, that are
    blacklisted (in crossref but leading nowhere).
    """
    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return CrossrefDOITable(begin=self.begin, date=self.date)

    def run(self):
        filepath = self.config.get('crossref', 'doi-blacklist', 'no-blacklist-available')

        # do not fail, if there is no blacklist configured
        if filepath == 'no-blacklist-available':
            self.logger.warning('no DOI blacklist configured, cf. #5706')
            self.input().copy(self.output().path)
            return

        # do not fail even, if file is configured but missing
        if not os.path.exists(filepath):
            self.logger.warn('crossref DOI blacklist configured, but file is missing: %s', filepath)
            self.input().copy(self.output().path)
            return

        blacklisted = set()

        with open(filepath) as handle:
            for line in (line.strip() for line in handle):
                blacklisted.add(line)

        with self.input().open() as handle:
            with self.output().open('w') as output:
                for line in (line.strip() for line in handle):
                    parts = line.split('\t')
                    if len(parts) < 3:
                        continue
                    if parts[2] in blacklisted:
                        continue
                    output.write(line + "\n")

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist.gz'), format=Gzip)


class CrossrefSortedDOITable(CrossrefTask):
    """
    Sort the CrossrefDOITable by filename and line number.
    """
    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return CrossrefDOITableClean(begin=self.begin, date=self.closest())

    def run(self):
        output = shellout("""
            TMPDIR={tmpdir} LC_ALL=C sort -S50% -k2,2 -k1,1n <(TMPDIR={tmpdir} unpigz -c {input}) | cut -f 1-2 | pigz -c > {output}""",
                          tmpdir=self.config.get('core', 'tempdir'), input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist.gz'), format=Gzip)


class CrossrefUniqItems(CrossrefTask):
    """
    What is left to do for calculating a Crossref
    snapshot: filter out the relevant lines.
    """
    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return CrossrefSortedDOITable(begin=self.begin, date=self.closest())

    def run(self):
        """
        We write line numbers to a separate file
        (L). We temporarily extract the chunk file
        as well (F), because pipes seem to fail
        randomly with many large files. Then we
        can run `filterline L F`.
        """
        _, stopover = tempfile.mkstemp(prefix='siskin-')

        previous_filename = None
        linenumbers = []

        with self.input().open() as handle:
            for line in handle:
                lineno, filename = line.strip().split('\t')
                if previous_filename and previous_filename != filename:
                    _, tmp = tempfile.mkstemp(prefix='siskin-')
                    with luigi.LocalTarget(tmp, format=TSV).open('w') as handle:
                        for lno in linenumbers:
                            handle.write_tsv(lno)

                    self.logger.info("filtering out %s lines", len(linenumbers))

                    input = shellout("unpigz -c {file} > {output}", file=previous_filename)
                    shellout("filterline {lines} {input} | pigz -c >> {output}",
                             filterline=self.assets('filterline'), lines=tmp, input=input, output=stopover)

                    try:
                        os.remove(tmp)
                        os.remove(input)
                    except OSError as err:
                        self.logger.warning(err)

                    linenumbers = []

                linenumbers.append(lineno)
                previous_filename = filename

        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))


class CrossrefIntermediateSchema(CrossrefTask):
    """
    Convert to intermediate format via span.
    """
    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'span': Executable(name='span-import', message='http://git.io/vI8NV'),
            'file': CrossrefUniqItems(begin=self.begin, date=self.date)
        }

    @timed
    def run(self):
        output = shellout("span-import -i crossref <(unpigz -c {input}) | pigz -c > {output}",
                          input=self.input().get('file').path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))


class CrossrefExport(CrossrefTask):
    """
    Tag with ISILs, then export to various formats.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='solr5vu3', description='export format')

    def requires(self):
        return {
            'file': CrossrefIntermediateSchema(date=self.date),
            'config': AMSLFilterConfig(date=self.date),
        }

    def run(self):
        output = shellout("span-tag -c {config} <(unpigz -c {input}) | pigz -c > {output}",
                          config=self.input().get('config').path, input=self.input().get('file').path)
        output = shellout("span-export -o {format} <(unpigz -c {input}) | pigz -c > {output}", format=self.format, input=output)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        extensions = {
            'solr5vu3': 'ldj.gz',
            'formeta': 'form.gz',
        }
        return luigi.LocalTarget(path=self.path(ext=extensions.get(self.format, 'gz')))


class CrossrefCollections(CrossrefTask):
    """
    A collection of crossref collections, refs. #6985.
    """
    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'input': CrossrefIntermediateSchema(begin=self.begin, date=self.date),
                'jq': Executable(name='jq', message='https://github.com/stedolan/jq')}

    @timed
    def run(self):
        output = shellout("""jq -r '.["finc.mega_collection"]?' <(unpigz -c {input}) | LC_ALL=C sort -S35% -u > {output}""",
                          input=self.input().get('input').path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class CrossrefCollectionsDifference(CrossrefTask):
    """
    Refs. #7049. Check list of collections against AMSL Crossref collections and
    report difference.
    """
    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'crossref': CrossrefCollections(begin=self.begin, date=self.date),
            'amsl': AMSLService(date=self.date, name='outboundservices:discovery')
        }

    @timed
    def run(self):
        amsl = set()

        with self.input().get('amsl').open() as handle:
            items = json.load(handle)

        for item in items:
            if item['sourceID'] == '49':
                amsl.add(item['collectionLabel'].strip())

        with self.input().get('crossref').open() as handle:
            with self.output().open('w') as output:
                for row in handle.iter_tsv(cols=('name',)):
                    if row.name not in amsl:
                        output.write_tsv(row.name)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class CrossrefDOIList(CrossrefTask):
    """
    A list of Crossref DOIs.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'input': CrossrefIntermediateSchema(date=self.date),
                'jq': Executable(name='jq', message='https://github.com/stedolan/jq')}

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        # process substitution sometimes results in a broken pipe, so extract beforehand
        output = shellout("unpigz -c {input} > {output}", input=self.input().get('input').path)
        shellout("""jq -r '.doi?' {input} | grep -o "10.*" 2> /dev/null | LC_ALL=C sort -S50% > {output} """,
                 input=output, output=stopover)
        os.remove(output)
        output = shellout("""sort -S50% -u {input} > {output} """, input=stopover)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class CrossrefISSNList(CrossrefTask):
    """
    Just dump a list of all ISSN values. With dups and all.
    """
    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'input': CrossrefUniqItems(begin=self.begin, date=self.date),
                'jq': Executable(name='jq', message='https://github.com/stedolan/jq')}

    @timed
    def run(self):
        output = shellout("jq -r '.ISSN[]?' <(unpigz -c {input}) 2> /dev/null > {output}", input=self.input().get('input').path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class CrossrefUniqISSNList(CrossrefTask):
    """
    Just dump a list of all ISSN values. Sorted and uniq.
    """
    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return CrossrefISSNList(begin=self.begin, date=self.date)

    @timed
    def run(self):
        output = shellout("sort -u {input} > {output}", input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class CrossrefDOIAndISSNList(CrossrefTask):
    """
    A list of Crossref DOIs with their ISSNs.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'input': CrossrefIntermediateSchema(date=self.date),
                'jq': Executable(name='jq', message='https://github.com/stedolan/jq')}

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        temp = shellout("unpigz -c {input} > {output}", input=self.input().get('input').path)
        output = shellout("""jq -r '[.doi?, .["rft.issn"][]?, .["rft.eissn"][]?] | @csv' {input} | LC_ALL=C sort -S50% > {output} """,
                          input=temp, output=stopover)
        os.remove(temp)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='csv'))


class CrossrefIndex(CrossrefTask, ElasticsearchMixin):
    """
    Vanilla records into elasticsearch. All or
    nothing.
    """
    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())
    index = luigi.Parameter(default='crossref')

    def requires(self):
        return CrossrefUniqItems(begin=self.begin, date=self.date)

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
        es.indices.put_mapping(index=self.index, doc_type='default', body=mapping)
        shellout("esbulk -verbose -index {index} {input}", index=self.index, input=self.input().path)

        with self.output().open('w'):
            pass

    def output(self):
        return luigi.LocalTarget(path=self.path())


class CrossrefHarvestGeneric(CrossrefTask):
    """
    Basic harvest of members, funders, etc.
    """
    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())
    kind = luigi.Parameter(default='members')

    rows = luigi.IntParameter(default=1000, significant=False)
    max_retries = luigi.IntParameter(default=10, significant=False)

    @timed
    def run(self):
        cache = URLCache(directory=os.path.join(tempfile.gettempdir(), '.urlcache'))
        adapter = requests.adapters.HTTPAdapter(max_retries=self.max_retries)
        cache.sess.mount('http://', adapter)

        rows, offset = self.rows, 0

        with self.output().open('w') as output:
            while True:
                params = {"rows": rows, "offset": offset}
                url = 'http://api.crossref.org/%s?%s' % (self.kind, urllib.urlencode(params))
                for attempt in range(1, 3):
                    body = cache.get(url)
                    try:
                        content = json.loads(body)
                    except ValueError as err:
                        if attempt == 2:
                            self.logger.debug("URL was %s", url)
                            self.logger.debug(err)
                            self.logger.debug(body[:100])
                            raise
                        if os.path.exists(cache.get_cache_file(url)):
                            self.logger.debug("trying to recover by removing cached entry")
                            os.remove(cache.get_cache_file(url))
                    else:
                        break
                items = content["message"]["items"]
                self.logger.debug("%s: %s", url, len(items))
                if len(items) == 0:
                    break
                output.write(body)
                output.write("\n")
                offset += rows

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))


class CrossrefGenericItems(CrossrefTask):
    """
    Flatten and deduplicate. Stub.
    """
    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())
    kind = luigi.Parameter(default='members')

    def requires(self):
        return CrossrefHarvestGeneric(begin=self.begin, date=self.closest(), kind=self.kind)

    @timed
    def run(self):
        with self.output().open('w') as output:
            with self.input().open() as handle:
                for line in handle:
                    content = json.loads(line)
                    if not content.get("status") == "ok":
                        raise RuntimeError("invalid response status")
                    items = content["message"]["items"]
                    for item in items:
                        output.write(json.dumps(item))
                        output.write("\n")

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))


class CrossrefDOIHarvest(CrossrefTask):
    """
    Harvest DOI redirects from doi.org API. This is a long running task. It's
    probably best to run this tasks complete separate from the rest and let
    other processing pipelines use the result, as it is available.

    ----

    It is highly recommended that you put a static entry into /etc/hosts
    for doi.org while `hurrly` is running.

    As of 2015-07-31 doi.org resolves to six servers. Just choose one.

    $ nslookup doi.org

    """

    def requires(self):
        """
        If we have more DOI sources, we could add them as requirements here.
        """
        return {
            'input': CrossrefDOIList(date=datetime.date.today()),
            'hurrly': Executable(name='hurrly', message='http://github.com/miku/hurrly'),
            'pigz': Executable(name='pigz', message='http://zlib.net/pigz/')
        }

    def run(self):
        output = shellout("hurrly -w 64 < {input} | pigz > {output}", input=self.input().get('input').path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='tsv.gz'))


class CrossrefDOIBlacklist(CrossrefTask):
    """
    Create a blacklist of DOIs. Possible cases:

    1. A DOI redirects to http://www.crossref.org/deleted_DOI.html or
       most of these sites below crossref.org: https://gist.github.com/miku/6d754104c51fb553256d
    2. A DOI API lookup does not return a HTTP 200.

    The output of this task should be used as doi-blacklist in config.
    """

    def requires(self):
        return CrossrefDOIHarvest()

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout("""LC_ALL=C zgrep -E "http(s)?://.*.crossref.org" {input} >> {output}""",
                 input=self.input().path, output=stopover)
        shellout("""LC_ALL=C zgrep -v "^200" {input} >> {output}""",
                 input=self.input().path, output=stopover)
        output = shellout("sort -S50% -u {input} | cut -f4 | sed s@http://doi.org/api/handles/@@g > {output}", input=stopover)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())
