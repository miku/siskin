#!/usr/bin/env python

"""
CrossRef is an association of scholarly publishers that develops shared
infrastructure to support more effective scholarly communications.

Our citation-linking network today covers over 68 million journal articles
and other content items (books chapters, data, theses, technical reports)
from thousands of scholarly and professional publishers around the globe.
"""

from gluish.benchmark import timed
from gluish.common import ElasticsearchMixin
from gluish.format import TSV
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import date_range, shellout
from siskin.task import DefaultTask
from siskin.utils import URLCache
import datetime
import elasticsearch
import json
import luigi
import os
import requests
import tempfile
import urllib

class CrossrefTask(DefaultTask):
    """
    Crossref related tasks. See: http://www.crossref.org/
    """
    TAG = 'crossref'

    def closest(self):
        """ Update frequency. """
        return monthly(date=self.date)

class CrossrefHarvestChunk(CrossrefTask):
    """
    API docs can be found under: http://api.crossref.org/

    The output file is line delimited JSON, just the concatenated responses.
    """
    begin = luigi.DateParameter()
    end = luigi.DateParameter()
    filter = luigi.Parameter(default='deposit', description='index, deposit, update')

    rows = luigi.IntParameter(default=1000, significant=False)
    max_retries = luigi.IntParameter(default=10, significant=False)

    @timed
    def run(self):
        cache = URLCache(directory=os.path.join(tempfile.gettempdir(), '.urlcache'))
        adapter = requests.adapters.HTTPAdapter(max_retries=self.max_retries)
        cache.sess.mount('http://', adapter)

        filter = "from-{self.filter}-date:{self.begin},until-{self.filter}-date:{self.end}".format(self=self)
        rows, offset = self.rows, 0

        with self.output().open('w') as output:
            while True:
                params = {"rows": rows, "offset": offset, "filter": filter}
                url = 'http://api.crossref.org/works?%s' % (urllib.urlencode(params))
                body = cache.get(url)
                try:
                    content = json.loads(body)
                except ValueError as err:
                    self.logger.debug("%s: %s" % (err, body))
                    raise
                items = content["message"]["items"]
                self.logger.debug("%s: %s" % (url, len(items)))
                if len(items) == 0:
                    break
                output.write(body)
                output.write("\n")
                offset += rows

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class CrossrefHarvest(luigi.WrapperTask, CrossrefTask):
    """
    Harvest everything in incremental steps. Yield the targets sorted by date,
    the latest chunks first. This way we can simply drop outdated records in later
    steps.
    """
    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    end = luigi.DateParameter(default=datetime.date.today())
    update = luigi.Parameter(default='month', description='days, weeks or months')

    def requires(self):
        if self.update not in ('days', 'weeks', 'months'):
            raise RuntimeError('update can only be: days, weeks or months')
        dates = date_range(self.begin, self.end, 1, self.update)
        tasks = [CrossrefHarvestChunk(begin=dates[i], end=dates[i + 1], rows=self.rows, filter=self.filter)
                 for i in range(len(dates) - 1)]
        return reversed(tasks)

    def output(self):
        return self.input()

class CrossrefItems(CrossrefTask):
    """
    Combine all harvested files into a single LDJ file.
    Flatten and deduplicate. Stub.
    """
    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return CrossrefHarvest(begin=self.begin, end=self.closest(), rows=self.rows, filter=self.filter)

    @timed
    def run(self):
        """ Extract all items from chunks. """
        with self.output().open('w') as output:
            for target in self.input():
                with target.open() as handle:
                    for line in handle:
                        content = json.loads(line)
                        if not content.get("status") == "ok":
                            raise RuntimeError("invalid response status: %s" % content)
                        items = content["message"]["items"]
                        for item in items:
                            output.write(json.dumps(item))
                            output.write("\n")

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class CrossrefUniqItems(CrossrefTask):
    """ Raw deduplication of crossref items. """
    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return CrossrefItems(begin=self.begin, date=self.date, rows=self.rows, filter=self.filter)

    @timed
    def run(self):
        output = shellout("TMPDIR={tmpdir} sort {input} | uniq > {output}", tmpdir=tempfile.gettempdir(), input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class CrossrefISSNList(CrossrefTask):
    """ Just dump a list of all ISSN values. With dups and all. """
    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return CrossrefUniqItems(begin=self.begin, date=self.date, filter=self.filter)

    @timed
    def run(self):
        output = shellout("jq -r '.ISSN[]' {input} 2> /dev/null > {output}", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class CrossrefUniqISSNList(CrossrefTask):
    """ Just dump a list of all ISSN values. Sorted and uniq. """
    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return CrossrefISSNList(begin=self.begin, date=self.date, filter=self.filter)

    @timed
    def run(self):
        output = shellout("sort -u {input} > {output}", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class CrossrefIndex(CrossrefTask, ElasticsearchMixin):
    """ Vanilla records. """

    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())
    index = luigi.Parameter(default='crossref')

    def requires(self):
        return CrossrefUniqItems(begin=self.begin, date=self.date, filter=self.filter)

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

class CrossrefAttributeList(CrossrefTask):
    """ Just export a list of a single attribute. Multiple values are written one per line.
    Results are sorted and deduplicated. """

    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())
    index = luigi.Parameter(default='crossref')
    attribute = luigi.Parameter(default='ISSN', description='URL, DOI, type, indexed.datestamp, publisher, ...')

    def requires(self):
        return CrossrefIndex(begin=self.begin, date=self.date, filter=self.filter)

    @timed
    def run(self):
        output = shellout("""estab -indices {index} -f "{attribute}" -1 | LANG=C grep -v NOT_AVAILABLE | LANG=C sort | LANG=C uniq > {output}""",
                          attribute=self.attribute, index=self.index, input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class CrossrefContainerList(CrossrefTask):
    """ Output (DOI, title, container-title) list. """

    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())
    index = luigi.Parameter(default='crossref')

    def requires(self):
        return CrossrefIndex(begin=self.begin, date=self.date, filter=self.filter)

    @timed
    def run(self):
        output = shellout("""estab -indices {index} -f "DOI title container-title" > {output}""", index=self.index, input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class CrossrefSolrJson(CrossrefTask):
    """ A first stab at JSON to JSON transformation for Solr. """

    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return CrossrefUniqItems(begin=self.begin, date=self.date, filter=self.filter)

    @timed
    def run(self):
        raise NotImplementedError("Use span manually for now. Crossref input: %s" % self.input().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class CrossrefSolrIndex(CrossrefTask):
    """ Index into solr. """

    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())
    limit = luigi.IntParameter(default=0)

    w = luigi.IntParameter(default=8, description='solrbulk worker', significant=False)
    size = luigi.IntParameter(default=10000, description='solrbulk batch size', significant=False)

    host = luigi.Parameter(default='localhost')
    port = luigi.IntParameter(default=8983)

    def requires(self):
        return CrossrefSolrJson(begin=self.begin, date=self.date, filter=self.filter, limit=self.limit)

    @timed
    def run(self):
        output = shellout("solrbulk -host {host} -port {port} -reset", host=self.host, port=self.port, input=self.input().path)
        output = shellout("solrbulk -host {host} -port {port} -w {w} -size {size} {input}", host=self.host, port=self.port, input=self.input().path)
        with self.output().open('w'):
            pass

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class CrossrefHarvestGeneric(CrossrefTask):
    """ Basic harvest of members, funders, etc. """
    begin = luigi.DateParameter(default=datetime.date(2006, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())
    kind = luigi.Parameter(default='members')

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
                body = cache.get(url)
                try:
                    content = json.loads(body)
                except ValueError as err:
                    self.logger.debug(err)
                    self.logger.debug(body)
                    raise
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
        return CrossrefHarvestGeneric(begin=self.begin, date=self.closest(), rows=self.rows)

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
    """ Determine coverage of ISSNs. Coverage means, there is at least one
    article with a given ISSN in crossref. Not included in the analysis
    are actual articles and coverage ranges. """
    date = ClosestDateParameter(default=datetime.date.today())

    isil = luigi.Parameter(description='isil, for meaningful file names')
    hfile = luigi.Parameter(description='path to a holdings file', significant=False)

    def requires(self):
        return CrossrefUniqISSNList(date=self.date)

    def run(self):
        """ This contains things, that would better be factored out in separate tasks. """
        titles = {}
        with luigi.File(shellout("span-gh-dump {hfile} > {output}", hfile=self.hfile), format=TSV).open() as handle:
            for row in handle.iter_tsv(cols=('issn', 'title')):
                titles[row.issn] = row.title

        issns_held = set()
        with luigi.File(shellout("xmlstarlet sel -t -v '//issn' {hfile} | sort | uniq > {output}", hfile=self.hfile), format=TSV).open() as handle:
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
