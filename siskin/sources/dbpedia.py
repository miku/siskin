# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

from gluish.benchmark import timed
from gluish.common import Executable, ElasticsearchMixin
from gluish.esindex import CopyToIndex
from gluish.format import TSV
from gluish.path import iterfiles
from gluish.utils import shellout, random_string
from siskin.task import DefaultTask
import elasticsearch
import hashlib
import luigi
import os
import pprint
import shutil
import tempfile

class DBPTask(DefaultTask):
    TAG = 'dbpedia'

    base = luigi.Parameter(default='http://downloads.dbpedia.org',
                           description='base url, for 2014 version, try http://data.dws.informatik.uni-mannheim.de/dbpedia',
                           significant=False)

class DBPDownload(DBPTask):
    """ Download DBPedia version and language.
    2014 versions seems to be hosted under

    """
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
                          {base}/{version}/{language}/ """,
                          base=self.base, prefix=target, format=self.format,
                          version=self.version, language=self.language)

        with self.output().open('w') as output:
            for path in sorted(iterfiles(target)):
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)

class DBPExtract(DBPTask):
    """ Extract all compressed files. """
    version = luigi.Parameter(default="3.9")
    language = luigi.Parameter(default="en")
    format = luigi.Parameter(default="nt", description="nq, nt, tql, ttl")

    def requires(self):
        return DBPDownload(base=self.base, version=self.version, language=self.language, format=self.format)

    def run(self):
        target = os.path.join(self.taskdir(), self.version, self.language, self.format)
        if not os.path.exists(target):
            os.makedirs(target)

        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                if not row.path.endswith('%s.bz2' % self.format):
                    self.logger.debug('skipping non bz2 file: %s' % row.path)
                    continue
                basename = os.path.basename(row.path)
                dst = os.path.join(target, os.path.splitext(basename)[0])
                if not os.path.exists(dst):
                    shellout("pbzip2 -d -m1000 -c {src} > {dst}", src=row.path, dst=dst)

        with self.output().open('w') as output:
            for path in sorted(iterfiles(target)):
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)

class DBPImages(DBPTask):
    """ Return a file with about 8M foaf:depictions. """
    version = luigi.Parameter(default="3.9")
    language = luigi.Parameter(default="en")
    format = luigi.Parameter(default="nt", description="nq, nt, tql, ttl")

    def requires(self):
        return DBPExtract(base=self.base, version=self.version, language=self.language, format=self.format)

    def run(self):
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                if row.path.endswith('images_%s.%s' % (self.language, self.format)):
                    filtered = shellout("""LANG=C grep -F "<http://xmlns.com/foaf/0.1/depiction>" {input} > {output}""", input=row.path)
                    luigi.File(filtered).copy(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext=self.format))

class DBPCategories(DBPTask):
    """ Return a file with categories and their labels. """
    version = luigi.Parameter(default="3.9")
    language = luigi.Parameter(default="en")
    format = luigi.Parameter(default="nt", description="nq, nt, tql, ttl")

    def requires(self):
        return DBPExtract(base=self.base, version=self.version, language=self.language, format=self.format)

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                if row.path.endswith('/article_categories_%s.%s' % (self.language, self.format)):
                    shellout("""cat {input} >> {output}""", input=row.path, output=stopover)
                if row.path.endswith('/category_labels_%s.%s' % (self.language, self.format)):
                    shellout("""cat {input} >> {output}""", input=row.path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext=self.format))

class DBPInterlanguageBacklinks(DBPTask):
    """ Extract interlanguage links pointing to the english dbpedia resource.
        Example output line:

    <http://de.dbpedia.org/resource/Vokov> <http://www.w3.org/2002/07/owl#sameAs> <http://dbpedia.org/resource/Vokov> .
    """
    version = luigi.Parameter(default="3.9")
    language = luigi.Parameter(default="de")
    format = luigi.Parameter(default="nt", description="nq, nt, tql, ttl")

    def requires(self):
        return DBPExtract(base=self.base, version=self.version, language=self.language, format=self.format)

    def run(self):
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                if row.path.endswith('/interlanguage_links_%s.%s' % (self.language, self.format)):
                    filtered = shellout("""LANG=C grep -F "<http://dbpedia.org/resource/" {input} > {output}""", input=row.path)
                    luigi.File(filtered).copy(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext=self.format))

class DBPTripleMelange(DBPTask):
    """ Combine several slices of triples from DBP for KG. """
    version = luigi.Parameter(default="3.9")

    def requires(self):
        return {
            'images-en': DBPImages(base=self.base, language='en', version=self.version),
            'images-de': DBPImages(base=self.base, language='de', version=self.version),
            'categories-en': DBPCategories(base=self.base, language='en', version=self.version),
            'categories-de': DBPCategories(base=self.base, language='de', version=self.version),
            'links': DBPInterlanguageBacklinks(base=self.base, language='de', version=self.version),
        }

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for _, target in self.input().iteritems():
            shellout("cat {input} >> {output}", input=target.path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='nt'))

class DBPCount(DBPTask):
    """ Just count the number of triples and store it. """
    version = luigi.Parameter(default="3.9")

    def requires(self):
        return DBPTripleMelange(base=self.base, version=self.version)

    @timed
    def run(self):
        output = shellout("wc -l {input} | awk '{{print $1}}' > {output}", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='count'))

class DBPTriplesSplitted(DBPTask):
    """ Split Ntriples into chunks, so we see some progress in virtuoso. """

    version = luigi.Parameter(default="3.9")
    lines = luigi.IntParameter(default=1000000)

    def requires(self):
        return DBPTripleMelange(base=self.base, version=self.version)

    @timed
    def run(self):
        prefix = '{0}-'.format(random_string())
        output = shellout("cd {tmp} && split -l {lines} -a 8 {input} {prefix} && cd -",
                          lines=self.lines, tmp=tempfile.gettempdir(),
                          input=self.input().path, prefix=prefix)
        target = os.path.join(self.taskdir())
        if not os.path.exists(target):
            os.makedirs(target)
        with self.output().open('w') as output:
            for path in iterfiles(tempfile.gettempdir()):
                filename = os.path.basename(path)
                if filename.startswith(prefix):
                    dst = os.path.join(target, filename)
                    shutil.move(path, dst)
                    output.write_tsv(dst)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)

class DBPVirtuoso(DBPTask):
    """ Load DBP parts into virtuoso 6. Assume there is no graph yet.
    Clear any graph manually with `SPARQL CLEAR GRAPH <http://dbpedia.org/resource/>;`

    See SELECT * FROM DB.DBA.LOAD_LIST; for progress.

    Also add `dirname $(taskoutput DBPTriplesSplitted)` to
    AllowedDirs in /etc/virtuoso-opensource-6.1/virtuoso.ini
    """

    version = luigi.Parameter(default="3.9")
    graph = luigi.Parameter(default='http://dbpedia.org/resource/')
    host = luigi.Parameter(default='localhost', significant=False)
    port = luigi.IntParameter(default=1111, significant=False)
    username = luigi.Parameter(default='dba', significant=False)
    password = luigi.Parameter(default='dba', significant=False)

    def requires(self):
        return DBPTriplesSplitted(base=self.base, version=self.version)

    @timed
    def run(self):
        with self.input().open() as handle:
            path = handle.iter_tsv(cols=('path',)).next().path
            dirname = os.path.dirname(path)
            filename = os.path.basename(path)
            prefix, id = filename.split('-')
        os.chmod(dirname, 0777)
        shellout(""" echo "LD_DIR ('{dirname}', '{prefix}-*', '{name}'); RDF_LOADER_RUN();" | isql-vt {host}:{port} {username} {password}""",
                 dirname=dirname, prefix=prefix, name=self.graph, host=self.host, port=self.port, username=self.username, password=self.password)

    def complete(self):
        """ Compare the expected number of triples with the number of loaded ones. """
        output = shellout("""curl -s -H 'Accept: text/csv' {host}:8890/sparql
                             --data-urlencode 'query=SELECT COUNT(*) FROM <{graph}> WHERE {{?a ?b ?c}}' |
                             grep -v callret > {output}""", host=self.host, port=self.port, graph=self.graph)

        with open(output) as handle:
            loaded = int(handle.read().strip())

        task = DBPCount(base=self.base, version=self.version)
        luigi.build([task], local_scheduler=True)
        with task.output().open() as handle:
            expected = int(handle.read().strip())
            if expected > 0 and loaded == 0:
                return False
            elif expected == loaded:
                return True
            else:
                raise RuntimeError('expected %s triples but loaded %s' % (expected, loaded))

class DBPPredicateDistribution(DBPTask):
    """ Just a uniq -c on the predicate 'column' """
    version = luigi.Parameter(default="3.9")
    language = luigi.Parameter(default="en")

    def requires(self):
        return DBPExtract(base=self.base, version=self.version, language=self.language)

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
        return DBPExtract(base=self.base, version=self.version, language=self.language, format='nt')

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
        return DBPExtract(base=self.base, version=self.version, language=self.language,
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
        return DBPJson(base=self.base, version=self.version, language=self.language)

class DBPRawImages(DBPTask, ElasticsearchMixin):
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
        return DBPDepictions(base=self.base)

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
        return DBPExtract(base=self.base, version=self.version, language=self.language, format=self.format)

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
