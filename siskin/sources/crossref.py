#!/usr/bin/env python
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
CrossRef is an association of scholarly publishers that develops shared
infrastructure to support more effective scholarly communications.

Our citation-linking network today covers over 68 million journal articles
and other content items (books chapters, data, theses, technical reports)
from thousands of scholarly and professional publishers around the globe.
"""

from gluish.common import Executable
from gluish.format import TSV, Gzip
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import date_range, shellout
from siskin.benchmark import timed
from siskin.configuration import Config
from siskin.sources.degruyter import DegruyterDOIList
from siskin.task import DefaultTask
from siskin.utils import URLCache, ElasticsearchMixin
import collections
import datetime
import elasticsearch
import json
import luigi
import os
import requests
import siskin
import tempfile
import urllib

config = Config.instance()

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

class CrossrefHarvestChunk(CrossrefTask):
    """
    Harvest a slice of Crossref.

    API docs can be found under: http://api.crossref.org/

    The output file is line delimited JSON, just the concatenated responses.
    """
    begin = luigi.DateParameter()
    end = luigi.DateParameter()
    filter = luigi.Parameter(default='deposit', description='index, deposit, update')

    rows = luigi.IntParameter(default=1000, significant=False)
    max_retries = luigi.IntParameter(default=10, significant=False, description='HTTP retries')
    attempts = luigi.IntParameter(default=3, significant=False, description='number of attempts to GET an URL that failed')

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
                    body = cache.get(url)
                    try:
                        content = json.loads(body)
                    except ValueError as err:
                        if attempt == self.attempts - 1:
                            self.logger.debug("URL was %s" % url)
                            self.logger.debug(err)
                            self.logger.debug(body[:100])
                            raise
                        if os.path.exists(cache.get_cache_file(url)):
                            self.logger.debug("trying to recover by removing cached entry")
                            os.remove(cache.get_cache_file(url))
                    else:
                        break
                items = content["message"]["items"]
                self.logger.debug("%s: %s" % (url, len(items)))
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
        tasks = [CrossrefHarvestChunk(begin=dates[i], end=dates[i + 1])
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
        return CrossrefHarvestChunk(begin=self.begin, end=self.end, filter=self.filter)

    def run(self):
        output = shellout("jq -c -r '.message.items[]?' {input} | pigz -c > {output}", input=self.input().path)
        luigi.File(output).move(self.output().path)

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
    """

    begin = luigi.DateParameter()
    end = luigi.DateParameter()
    filter = luigi.Parameter(default='deposit', description='index, deposit, update')

    def requires(self):
        return CrossrefChunkItems(begin=self.begin, end=self.end, filter=self.filter)

    def run(self):
        output = shellout(r"""jq -r '.DOI?' <(unpigz -c {input}) | awk '{{print NR"\t{input}\t"$0 }}' | pigz -c > {output}""", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='tsv.gz'))

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
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='tsv.gz'))

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
            TMPDIR={tmpdir} LC_ALL=C sort -k2,2 -S35% <(TMPDIR={tmpdir} unpigz -c {input}) | tac | TMPDIR={tmpdir} LC_ALL=C sort -S35% -u -k3,3 | pigz -c > {output}""",
            tmpdir=config.get('core', 'tempdir'), input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='tsv.gz'), format=Gzip)

class CrossrefDOITableClean(CrossrefTask):
    """
    Clean the DOI table from DOIs, that are blacklisted (in crossref but leading nowhere).
    """
    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        from siskin.sources.doi import DOIBlacklist
        return {
            'table': CrossrefDOITable(begin=self.begin, date=self.date),
            'blacklist': DOIBlacklist(date=self.date),
        }

    def run(self):
        blacklisted = set()
        with self.input().get('blacklist').open() as handle:
            for line in (line.strip() for line in handle):
                blacklisted.add(line)

        with self.input().get('table').open() as handle:
            with self.output().open('w') as output:
                for line in (line.strip() for line in handle):
                    parts = line.split('\t')
                    if len(parts) < 3:
                        continue
                    if parts[2] in blacklisted:
                        continue
                    output.write(line + "\n")

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='tsv.gz'), format=Gzip)

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
            tmpdir=config.get('core', 'tempdir'), input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='tsv.gz'), format=Gzip)

class CrossrefUniqItems(CrossrefTask):
    """
    What is left to do for calculating a Crossref snapshot: filter out the relevant lines.
    """
    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return CrossrefSortedDOITable(begin=self.begin, date=self.closest())

    def run(self):
        """
        We write line numbers to a separate file (L). We temporarily extract
        the chunk file as well (F). Then we can run `filterline L F`.
        """
        _, stopover = tempfile.mkstemp(prefix='siskin-')

        previous_filename = None
        linenumbers = []

        with self.input().open() as handle:
            for line in handle:
                lineno, filename = line.strip().split('\t')
                if previous_filename and previous_filename != filename:
                    _, tmp = tempfile.mkstemp(prefix='siskin-')
                    with luigi.File(tmp, format=TSV).open('w') as handle:
                        for ln in linenumbers:
                            handle.write_tsv(ln)

                    self.logger.info("filtering out %s lines" % len(linenumbers))

                    input = shellout("unpigz -c {file} > {output}", file=previous_filename)
                    shellout("filterline {lines} {input} | pigz -c >> {output}",
                             filterline=self.assets('filterline'), lines=tmp, input=input, output=stopover)

                    try:
                        os.remove(tmp)
                        os.remove(input)
                    except Exception as err:
                        self.logger.warning(err)

                    linenumbers = []

                linenumbers.append(lineno)
                previous_filename = filename

        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))

class CrossrefIntermediateSchema(CrossrefTask):
    """
    Convert to intermediate format via span.
    """
    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
       return {'span': Executable(name='span-import', message='http://git.io/vI8NV'),
               'file': CrossrefUniqItems(date=self.date)}

    @timed
    def run(self):
        output = shellout("span-import -i crossref <(unpigz -c {input}) | pigz -c > {output}", input=self.input().get('file').path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))

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
        shellout("""jq -r '.doi?' <(unpigz -c {input}) | grep -o "10.*" 2> /dev/null | LC_ALL=C sort -S50% > {output} """,
                 input=self.input().get('input').path, output=stopover)
        output = shellout("""sort -S50% -u {input} > {output} """, input=stopover)
        luigi.File(output).move(self.output().path)

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
        luigi.File(output).move(self.output().path)

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
        luigi.File(output).move(self.output().path)

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
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='csv'))

class CrossrefIndex(CrossrefTask, ElasticsearchMixin):
    """
    Vanilla records into elasticsearch.
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
        es.indices.put_mapping(index='bsz', doc_type='default', body=mapping)
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
                            self.logger.debug("URL was %s" % url)
                            self.logger.debug(err)
                            self.logger.debug(body[:100])
                            raise
                        if os.path.exists(cache.get_cache_file(url)):
                            self.logger.debug("trying to recover by removing cached entry")
                            os.remove(cache.get_cache_file(url))
                    else:
                        break
                items = content["message"]["items"]
                self.logger.debug("%s: %s" % (url, len(items)))
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

class CrossrefCoverage(CrossrefTask):
    """
    Determine coverage of ISSNs. Coverage means, there is at least one
    article with a given ISSN in crossref. Not included in the analysis
    are actual articles and coverage ranges.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    isil = luigi.Parameter(description='isil, for meaningful file names')
    hfile = luigi.Parameter(description='path to a holdings file', significant=False)

    def requires(self):
        return CrossrefUniqISSNList(date=self.date)

    def run(self):
        """
        TODO(miku): This contains things, that would better be factored out in separate tasks.
        """
        titles = {}
        output = shellout("span-gh-dump {hfile} > {output}", hfile=self.hfile)
        with luigi.File(output, format=TSV).open() as handle:
            for row in handle.iter_tsv(cols=('issn', 'title')):
                titles[row.issn] = row.title

        issns_held = set()
        output = shellout("xmlstarlet sel -t -v '//issn' {hfile} | sort | uniq > {output}", hfile=self.hfile)
        with luigi.File(output, format=TSV).open() as handle:
            for row in handle.iter_tsv(cols=('issn',)):
                issns_held.add(row.issn)

        issns_crossref = set()
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('issn',)):
                issns_crossref.add(row.issn)

        covered = issns_held.intersection(issns_crossref)

        with self.output().open('w') as output:
            for issn in issns_held:
                if issn in covered:
                    output.write_tsv("COVERED", issn, titles[issn])
                else:
                    output.write_tsv("NOT_COVERED", issn, titles[issn])

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
