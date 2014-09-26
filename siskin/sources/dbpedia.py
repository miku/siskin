# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

from gluish.benchmark import timed
from gluish.common import Executable, ElasticsearchMixin
from gluish.esindex import CopyToIndex
from gluish.format import TSV
from gluish.path import iterfiles
from gluish.utils import shellout
from siskin.task import DefaultTask
import elasticsearch
import hashlib
import luigi
import os
import pprint
import tempfile

class DBPTask(DefaultTask):
    TAG = 'dbpedia'

class DBPDownload(DBPTask):
    """ Download DBPedia version and language. """
    version = luigi.Parameter(default="3.9")
    language = luigi.Parameter(default="en")
    format = luigi.Parameter(default="nt", description="nq, nt, tql, ttl")

    def requires(self):
        return Executable(name='wget')

    def run(self):
        target = os.path.join(self.taskdir(), self.version, self.language, self.format)
        if not os.path.exists(target):
            os.makedirs(target)
        output = shellout(""" wget --retry-connrefused
                          -P {prefix} -nd -nH -np -r -c -A *{format}.bz2
                          http://downloads.dbpedia.org/{version}/{language}/ """,
                          prefix=target, format=self.format,
                          version=self.version, language=self.language)

        with self.output().open('w') as output:
            for path in sorted(iterfiles(target)):
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class DBPExtract(DBPTask):
    """ Extract all compressed files. """
    version = luigi.Parameter(default="3.9")
    language = luigi.Parameter(default="en")
    format = luigi.Parameter(default="nt", description="nq, nt, tql, ttl")

    def requires(self):
        return DBPDownload(version=self.version, language=self.language, format=self.format)

    def run(self):
        target = os.path.join(self.taskdir(), self.version, self.language, self.format)
        if not os.path.exists(target):
            os.makedirs(target)

        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                basename = os.path.basename(row.path)
                dst = os.path.join(target, os.path.splitext(basename)[0])
                shellout("pbzip2 -d -m1000 -c {src} > {dst}", src=row.path, dst=dst)

        with self.output().open('w') as output:
            for path in sorted(iterfiles(target)):
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class DBPPredicateDistribution(DBPTask):
    """ Just a uniq -c on the predicate 'column' """
    version = luigi.Parameter(default="3.9")
    language = luigi.Parameter(default="en")

    def requires(self):
        return DBPExtract(version=self.version, language=self.language)

    def run(self):
        output = shellout("""cut -d " " -f2 {input} | LANG=C sort | LANG=C uniq -c > {output}""",
                          input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='txt'))

class DBPAbbreviatedNTriples(DBPTask):
    """ Convert all DBPedia ntriples to a single JSON file. """

    version = luigi.Parameter(default="3.9")
    language = luigi.Parameter(default="en")

    def requires(self):
        return DBPExtract(version=self.version, language=self.language, format='nt')

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                # TODO: ignore these in a saner way ...
                if 'long_abstracts_' in row.path:
                    continue
                if '_unredirected' in row.path:
                    continue
                if '_cleaned' in row.path:
                    continue
                if 'old_' in row.path:
                    continue
                if 'revision_ids' in row.path:
                    continue
                output = shellout("ntto -a -r {rules} -o {output} {input}",
                                  rules=self.assets('RULES.txt'), input=row.path)
                shellout("cat {input} >> {output} && rm -f {input}", input=output, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='nt'))

class DBPJson(DBPTask):
    """ Convert all DBPedia ntriples to a single JSON file. """

    version = luigi.Parameter(default="3.9")
    language = luigi.Parameter(default="en")

    def requires(self):
        return DBPExtract(version=self.version, language=self.language,
                           format='nt')

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                # TODO: ignore these in a saner way ...
                if 'long_abstracts_' in row.path:
                    continue
                if '_unredirected' in row.path:
                    continue
                if '_cleaned' in row.path:
                    continue
                if 'old_' in row.path:
                    continue
                shellout("ntto -i -j -a -r {rules} {input} >> {output}",
                         input=row.path, rules=self.assets('RULES.txt'),
                         output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class DBPIndex(DBPTask, CopyToIndex):
    """ Index most of DBPedia into a single index. """

    version = luigi.Parameter(default="3.9")
    language = luigi.Parameter(default="en")

    index = 'dbp'
    chunk_size = 8196
    purge_existing_index = False
    timeout = 120

    @property
    def doc_type(self):
        return self.language

    def requires(self):
        return DBPJson(version=self.version, language=self.language)

class DBPImages(DBPTask, ElasticsearchMixin):
    """ Generate a raw list of (s, p, o) tuples that contain string ending with jpg. """

    index = luigi.Parameter(default='dbp', description='name of the index to search')

    def requires(self):
        return Executable(name='estab', message='http://git.io/bLY7cQ')

    def run(self):
        output = shellout(""" estab -indices {index} -f "s p o" -query '{{"query": {{"query_string": {{"query": "*jpg"}}}}}}' > {output}""", index=self.index)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class DBPDepictions(DBPTask, ElasticsearchMixin):
    """ Generate a raw list of (s, p, o) tuples that are foaf:depictions. """

    index = luigi.Parameter(default='dbp', description='name of the index to search')

    def requires(self):
        return Executable(name='estab', message='http://git.io/bLY7cQ')

    def run(self):
        output = shellout(r""" estab -indices {index} -f "s p o" -query '{{"query": {{"query_string": {{"query": "p:\"foaf:depiction\""}}}}}}' > {output}""", index=self.index)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class DBPDownloadDepictions(DBPTask):
    """ Download depictions from wikimedia. """

    def requires(self):
        return DBPDepictions()

    def run(self):
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('s', 'p', 'o')):
                url = row.o
                id = hashlib.sha1(url).hexdigest()
                shard, filename = id[:2], id[2:]
                target = os.path.join(self.taskdir(), shard, filename)
                if not os.path.exists(target):
                    output = shellout("""wget -O {output} "{url}" """, url=url, ignoremap={8: '404s throw 8'})
                    luigi.File(output).move(target)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

#
# Some ad-hoc task, TODO: cleanup or rework.
#
class DBPSameAs(DBPTask):
    """
    Extract all owl:sameAS relations from dbpedia.
    TODO: get rid of this or rework. """

    version = luigi.Parameter(default="3.9")
    language = luigi.Parameter(default="en")
    format = luigi.Parameter(default="nt", description="nq, nt, tql, ttl")

    def requires(self):
        return DBPExtract(version=self.version, language=self.language, format=self.format)

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                shellout('LANG=C grep "owl#sameAs" {path} >> {output}', path=row.path,
                         output=stopover, ignoremap={1: "Not found."})
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='n3'))

class DBPKnowledgeGraphLookup(DBPTask, ElasticsearchMixin):
    """ Example aggregation of things. """

    # version = luigi.Parameter(default="3.9")
    # language = luigi.Parameter(default="de")
    gnd = luigi.Parameter(default='118540238')

    @timed
    def run(self):
        """ Goethe 118540238 """
        es = elasticsearch.Elasticsearch([dict(host=self.es_host, port=self.es_port)])

        subjects = set()
        triples = set()

        r1 = es.search(index='dbp', doc_type='de', body={'query': {
                       'query_string': {'query': '"%s"' % self.gnd}}})
        for hit in r1['hits']['hits']:
            source = hit.get('_source')
            triples.add((source.get('s'), source.get('p'), source.get('o')))
            subjects.add(source.get('s'))

        for subject in subjects:
            r2 = es.search(index='dbp', doc_type='de', body={'query': {
                           'query_string': {'query': '"%s"' % subject}}}, size=4000)
            for hit in r2['hits']['hits']:
                source = hit.get('_source')
                triples.add((source.get('s'), source.get('p'), source.get('o')))

        with self.output().open('w') as output:
            for s, p, o in triples:
                output.write_tsv(s, p, o)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GraphLookup(DBPTask, ElasticsearchMixin):
    """ Display something in the shell. """

    gnd = luigi.Parameter(default='118540238')

    def requires(self):
        return DBPKnowledgeGraphLookup(gnd=self.gnd, es_host=self.es_host, es_port=self.es_port)

    def run(self):
        widget = {}
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('s', 'p', 'o')):

                if row.p == 'do:birthPlace':
                    widget['geboren in'] = row.o
                if row.p == 'do:birthDate':
                    widget['geboren am'] = row.o
                if row.p == 'do:deathPlace':
                    widget['gestorben in'] = row.o
                if row.p == 'do:deathDate':
                    widget['gestorben am'] = row.o
                if row.p == 'dp:kurzbeschreibung':
                    widget['info'] = row.o
                if row.p == 'dp.de:alternativnamen':
                    widget['alias'] = row.o

        pprint.pprint(widget)

    def complete(self):
        return False

class DBPGNDLinks(DBPTask, ElasticsearchMixin):
    """ Find all links from DBP to GND via dp.de:gnd """

    index = luigi.Parameter(default='dbp', description='name of the index to search')

    def requires(self):
        return Executable(name='estab', message='http://git.io/bLY7cQ')

    def run(self):
        output = shellout(r""" estab -indices {index} -f "s p o" -query '{{"query": {{"query_string": {{"query": "p:\"dp.de:gnd\""}}}}}}' > {output}""", index=self.index)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
