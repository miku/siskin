# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103

"""
GND-related tasks.
"""

from elasticsearch import helpers as eshelpers
from gluish.benchmark import timed
from gluish.common import ElasticsearchMixin
from gluish.esindex import CopyToIndex
from gluish.format import TSV
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.task import BaseTask
from gluish.utils import shellout
import BeautifulSoup
import collections
import cPickle as pickle
import datetime
import elasticsearch
import HTMLParser
import luigi
import prettytable
import requests
import urllib

class GNDTask(BaseTask):
    TAG = 'gnd'

    def closest(self):
        return monthly(self.date)

class DNBStatus(GNDTask):
    """ List available downloads. For human eyes only. """

    @timed
    def run(self):
        host = "datendienst.dnb.de"
        path = "/cgi-bin/mabit.pl"
        params = {
            "userID": "opendata",
            "pass": "opendata",
            "cmd": "login",
        }
        r = requests.get('http://{host}{path}?{params}'.format(host=host,
                         path=path, params=urllib.urlencode(params)))
        if not r.status_code == 200:
            raise RuntimeError(r)

        soup = BeautifulSoup.BeautifulSoup(r.text)
        tables = soup.findAll('table')
        if not tables:
            raise RuntimeError('No tabular data found on {}'.format(r.url))

        table = tables[0]
        h = HTMLParser.HTMLParser()

        rows = []
        for tr in table.findAll('tr'):
            row = []
            for td in tr.findAll('td'):
                text = ''.join(h.unescape(td.find(text=True)))
                row.append(text)
            rows.append(row)

        x = prettytable.PrettyTable(rows[0])
        for row in rows[1:]:
            x.add_row(row)
        print(x)

    def complete(self):
        return False

class GNDDump(GNDTask):
    """ Download the GND snapshot. """

    date = ClosestDateParameter(default=datetime.date.today())

    @timed
    def run(self):
        host = "datendienst.dnb.de"
        path = "/cgi-bin/mabit.pl"
        params = {
            "userID": "opendata",
            "pass": "opendata",
            "cmd": "fetch",
            "mabheft": "GND.ttl.gz",
        }

        url = 'http://{host}{path}?{params}'.format(host=host,
                                                    path=path,
                                                    params=urllib.urlencode(params))
        output = shellout("""wget --retry-connrefused "{url}" -O {output}""",
                          url=url)
        luigi.File(output).move(self.output().fn)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ttl.gz'))

class GNDExtract(GNDTask):
    """ Extract the archive. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GNDDump(date=self.date)

    @timed
    def run(self):
        output = shellout("gunzip -c {input} > {output}", input=self.input().fn)
        luigi.File(output).move(self.output().fn)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ttl'))

class GNDNTriples(GNDTask):
    """ Get a Ntriples representation of GND. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GNDExtract(date=self.date)

    @timed
    def run(self):
        output = shellout("serdi -b -i turtle -o ntriples {input} > {output}",
                          input=self.input().path)
        # output = shellout("sort {input} > {output}", input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='nt'))

class GNDJson(GNDTask):
    """ Convert to some indexable JSON. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GNDNTriples(date=self.date)

    @timed
    def run(self):
        output = shellout("nttoldj -a -i {input} > {output}", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class GNDIndex(GNDTask, CopyToIndex):
    date = ClosestDateParameter(default=datetime.date.today())

    index = 'gnd'
    doc_type = 'triples'
    purge_existing_index = True

    def requires(self):
        return GNDJson(date=self.date)

class GNDBroaderTerms(GNDTask, ElasticsearchMixin):
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        es = elasticsearch.Elasticsearch([dict(host=self.es_host,
                                               port=self.es_port)])
        hits = eshelpers.scan(es, {
            "query": {
                "query_string": {
                    "query": "*broaderTermGeneral"
                }
            }, 'fields': ["s", "o"]}, index='gnd', scroll='30m', size=10000)

        with self.output().open('w') as output:
            for hit in hits:
                fields = hit.get('fields')
                try:
                    output.write_tsv(fields['s'][0],fields['o'][0])
                except Exception:
                    pass

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GNDPreferredNames(GNDTask, ElasticsearchMixin):
    date = ClosestDateParameter(default=datetime.date.today())

    @timed
    def run(self):
        es = elasticsearch.Elasticsearch([dict(host=self.es_host,
                                               port=self.es_port)])
        hits = eshelpers.scan(es, {
            "query": {
                "query_string": {
                    "query": "*preferredNameForTheSubjectHeading"
                }
            }, 'fields': ["s", "o"]}, index='gnd', scroll='30m', size=10000)

        with self.output().open('w') as output:
            for hit in hits:
                fields = hit.get('fields')
                try:
                    output.write_tsv(fields['s'][0], fields['o'][0])
                except Exception:
                    pass

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GNDPreferredNamesPickled(GNDTask):
    """ For each term in broaderTermGeneral, save broader and narrower terms. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GNDPreferredNames(date=self.date)

    def run(self):
        names = {}
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('id', 'name')):
                names[row.id] = row.name

        with self.output().open('w') as output:
            pickle.dump(names, output)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='pkl'))

class GNDNarrowerTermsPickled(GNDTask):
    """ For each term in broaderTermGeneral, save broader and narrower terms. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GNDBroaderTerms(date=self.date)

    def run(self):
        down = collections.defaultdict(set)
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('narrower', 'broader')):
                down[row.broaders].add(row.narrower)

        with self.output().open('w') as output:
            pickle.dump(down, output)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='pkl'))

class GNDBroaderTermsPickled(GNDTask):
    """ For each term in broaderTermGeneral, save broader and narrower terms. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GNDBroaderTerms(date=self.date)

    def run(self):
        up = {}
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('narrower', 'broader')):
                up[row.narrower] = row.broader

        with self.output().open('w') as output:
            pickle.dump(up, output)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='pkl'))

class GNDRelatedTerms(GNDTask, ElasticsearchMixin):
    date = ClosestDateParameter(default=datetime.date.today())

    @timed
    def run(self):
        es = elasticsearch.Elasticsearch([dict(host=self.es_host,
                                               port=self.es_port)])
        hits = eshelpers.scan(es, {
            "query": {
                "query_string": {
                    "query": "*relatedTerm"
                }
            }, 'fields': ["s", "o"]}, index='gnd', scroll='30m', size=10000)

        with self.output().open('w') as output:
            for hit in hits:
                fields = hit.get('fields')
                try:
                    output.write_tsv(fields['s'][0],fields['o'][0])
                except Exception:
                    pass

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GNDTaxonomy(GNDTask):
    """ For each term in broaderTermGeneral, save broader and narrower terms. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GNDBroaderTerms(date=self.date)

    def run(self):
        taxonomy = {}
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('narrower', 'broader')):
                taxonomy[row.narrower] = row.broader
        with self.output().open('w') as output:
            for key, value in taxonomy.iteritems():
                broader = []
                while True:
                    broader.append(value)
                    if not value in taxonomy:
                        break
                    else:
                        value = taxonomy[value]
                        if value in broader:
                            break
                output.write_tsv(key, "|".join(broader))

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GNDSameAs(GNDTask, ElasticsearchMixin):
    """ Find all GNDs that link to some dbp id. """

    date = ClosestDateParameter(default=datetime.date.today())

    @timed
    def run(self):
        es = elasticsearch.Elasticsearch([dict(host=self.es_host,
                                               port=self.es_port)])
        hits = eshelpers.scan(es, {"query": {
                "filtered": {
                    "query": {
                        "match_all": {}
                    },
                    "filter": {
                        "query": {
                            "query_string": {
                                "default_field": "p",
                                "query": "*sameAs"
                            }
                        }
                    }
                }
            }, 'fields': ["s", "o"]}, index='gnd', scroll='30m', size=10000)

        with self.output().open('w') as output:
            for hit in hits:
                fields = hit.get('fields')
                try:
                    output.write_tsv(fields['s'][0],fields['o'][0])
                except Exception:
                    pass

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
