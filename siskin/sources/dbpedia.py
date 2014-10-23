# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

from gluish.benchmark import timed
from gluish.common import Executable, ElasticsearchMixin
from gluish.esindex import CopyToIndex
from gluish.format import TSV
from gluish.path import iterfiles
from gluish.utils import shellout, random_string
from siskin.task import DefaultTask
import collections
import datetime
import elasticsearch
import hashlib
import json
import luigi
import os
import pprint
import shutil
import tempfile

class DBPTask(DefaultTask):
    TAG = 'dbpedia'

class DBPDownload(DBPTask):
    """ Download DBPedia version and language.
    2014 versions hosted under http://data.dws.informatik.uni-mannheim.de/dbpedia.
    For other download locations adjust bases.

    """
    version = luigi.Parameter(default="2014")
    language = luigi.Parameter(default="en")
    format = luigi.Parameter(default="nt", description="nq, nt, tql, ttl")

    def requires(self):
        return Executable(name='wget')

    def run(self):
        bases = collections.defaultdict(lambda: 'http://downloads.dbpedia.org')
        bases['2014'] = 'http://data.dws.informatik.uni-mannheim.de/dbpedia'

        target = os.path.join(self.taskdir(), self.version, self.language, self.format)
        if not os.path.exists(target):
            os.makedirs(target)

        url = '{base}/{version}/{language}/'.format(base=bases[self.version], version=self.version, language=self.language)
        output = shellout(""" wget --retry-connrefused -P {prefix} -nd -nH -np -r -c -A *{format}.bz2 {url}""", url=url, prefix=target, format=self.format)

        pathlist = sorted(iterfiles(target))
        if len(pathlist) == 0:
            raise RuntimeError('no files downloaded, double check --base option')

        with self.output().open('w') as output:
            for path in pathlist:
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)

class DBPExtract(DBPTask):
    """ Extract all compressed files. """
    version = luigi.Parameter(default="2014")
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
                if not row.path.endswith('%s.bz2' % self.format):
                    self.logger.debug('skipping non bz2 file: %s' % row.path)
                    continue
                basename = os.path.basename(row.path)
                dst = os.path.join(target, os.path.splitext(basename)[0])
                if not os.path.exists(dst):
                    shellout("pbzip2 -d -m1000 -c {src} > {dst}", src=row.path, dst=dst)

        pathlist = sorted(iterfiles(target))
        if len(pathlist) == 0:
            raise RuntimeError('no files extracted, double check --base option')

        with self.output().open('w') as output:
            for path in pathlist:
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)

class DBPCategoryDistribution(DBPTask):
    """
    Something like this: https://gist.github.com/miku/78d84756d08d7dace204

    Another example from the english wikipedia:

         664354 Living people
          88726 Archived files for deletion discussions
          51015 Year of birth missing (living people)
          37800 Pending DYK nominations
          28772 English-language films
          20531 American films
          19743 Local COIBot Reports
          19606 Year of birth unknown
          18093 Year of birth missing
          17793 The Football League players
          17192 Year of death missing
        ...
    """
    version = luigi.Parameter(default="2014")
    language = luigi.Parameter(default="en")
    format = luigi.Parameter(default="nt", description="nq, nt, tql, ttl")

    def requires(self):
        return DBPCategories(version=self.version, language=self.language, format=self.format)

    @timed
    def run(self):
        output = shellout(r"""LANG=C cut -d ' ' -f 3 {input} |
                              LANG=C sort | LANG=C uniq -c |
                              LANG=C sort -nr > {output}""", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class DBPImages(DBPTask):
    """ Return a file with about 8M foaf:depictions. """
    version = luigi.Parameter(default="2014")
    language = luigi.Parameter(default="en")
    format = luigi.Parameter(default="nt", description="nq, nt, tql, ttl")

    def requires(self):
        return DBPExtract(version=self.version, language=self.language, format=self.format)

    def run(self):
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                if row.path.endswith('images_%s.%s' % (self.language, self.format)):
                    filtered = shellout("""LANG=C grep -F "<http://xmlns.com/foaf/0.1/depiction>" {input} > {output}""", input=row.path)
                    luigi.File(filtered).copy(self.output().path)
                    break
            else:
                raise RuntimeError('no file found')

    def output(self):
        return luigi.LocalTarget(path=self.path(ext=self.format))

class DBPAbstracts(DBPTask):
    """ Return a file with about rdfs:comment abstracts. """
    version = luigi.Parameter(default="2014")
    language = luigi.Parameter(default="en")
    format = luigi.Parameter(default="nt", description="nq, nt, tql, ttl")

    def requires(self):
        return DBPExtract(version=self.version, language=self.language, format=self.format)

    def run(self):
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                if row.path.endswith('short_abstracts_%s.%s' % (self.language, self.format)):
                    luigi.File(row.path).copy(self.output().path)
                    break
            else:
                raise RuntimeError('no file found')

    def output(self):
        return luigi.LocalTarget(path=self.path(ext=self.format))

class DBPCategories(DBPTask):
    """ Return a file with categories and their labels. """
    version = luigi.Parameter(default="2014")
    language = luigi.Parameter(default="en")
    format = luigi.Parameter(default="nt", description="nq, nt, tql, ttl")

    def requires(self):
        return DBPExtract(version=self.version, language=self.language, format=self.format)

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

class DBPCategoryTable(DBPTask):
    """
    Just as WikipediaCategoryTable compute a table of tuples
    of (page, category), here, with full URIs.
    """
    version = luigi.Parameter(default="2014")
    language = luigi.Parameter(default="en")

    def requires(self):
        return DBPCategories(version=self.version, language=self.language, format='nt')

    @timed
    def run(self):
        output = shellout("""LANG=C grep -vF "<http://www.w3.org/2000/01/rdf-schema#label>" {input} |
                             LANG=C cut -d ' ' -f1,3 |
                             LANG=C sed -e 's/> </>\\t</g' |
                             LANG=C grep -v "^#" > {output}""", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class DBPCategoryExtension(DBPTask):
    """ For a category, collect all pages/resources, that fall into that
    category. Format (category, page). """

    version = luigi.Parameter(default="2014")
    language = luigi.Parameter(default="en")

    def requires(self):
        return DBPCategoryTable(version=self.version, language=self.language)

    @timed
    def run(self):
        extension = collections.defaultdict(set)
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('page', 'category')):
                extension[row.category].add(row.page)

        with self.output().open('w') as output:
            for category, pages in extension.iteritems():
                for p in pages:
                    output.write_tsv(category, p)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class DBPCategoryExtensionGND(DBPTask):
    """ For a category, collect all pages/resources, that fall into that
    category. Format (category, page). Replace category and page with GND if possible. """

    version = luigi.Parameter(default="2014")
    language = luigi.Parameter(default="en")

    def requires(self):
        return {'cats': DBPCategoryExtension(version=self.version, language=self.language),
                'links': DBPGNDValidLinks(version=self.version, language=self.language)}

    def abbreviate_ns(self, s):
        pattern = {
            'de': '<http://de.dbpedia.org/resource/',
            'en': '<http://dbpedia.org/resource/',
        }
        return s.replace(pattern.get(self.language, 'en'), 'dbp:').rstrip('>')

    @timed
    def run(self):
        mappings = {}

        with self.input().get('links').open() as handle:
            for row in handle.iter_tsv(cols=('dbp', 'gnd')):
                mappings[row.dbp] = row.gnd

        with self.input().get('cats').open() as handle:
            with self.output().open('w') as output:
                for row in handle.iter_tsv(cols=('category', 'page')):
                    cc = mappings.get(self.abbreviate_ns(row.category), row.category)
                    pp = mappings.get(self.abbreviate_ns(row.page), row.page)
                    output.write_tsv(cc, pp)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class DBPInfobox(DBPTask):
    """ Use infobox properties. """
    version = luigi.Parameter(default="2014")
    language = luigi.Parameter(default="en")
    format = luigi.Parameter(default="nt", description="nq, nt, tql, ttl")

    def requires(self):
        return DBPExtract(version=self.version, language=self.language, format=self.format)

    def run(self):
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                if row.path.endswith('infobox_properties_%s.%s' % (self.language, self.format)):
                    luigi.File(row.path).copy(self.output().path)
                    break
            else:
                raise RuntimeError('no file found')

    def output(self):
        return luigi.LocalTarget(path=self.path(ext=self.format))

class DBPInterlanguageBacklinks(DBPTask):
    """ Extract interlanguage links pointing to the english dbpedia resource.
        Example output line:

    <http://de.dbpedia.org/resource/Vokov> <http://www.w3.org/2002/07/owl#sameAs> <http://dbpedia.org/resource/Vokov> .
    """
    version = luigi.Parameter(default="2014")
    language = luigi.Parameter(default="de")
    format = luigi.Parameter(default="nt", description="nq, nt, tql, ttl")

    def requires(self):
        return DBPExtract(version=self.version, language=self.language, format=self.format)

    def run(self):
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                if row.path.endswith('/interlanguage_links_%s.%s' % (self.language, self.format)):
                    filtered = shellout("""LANG=C grep -F "<http://dbpedia.org/resource/" {input} > {output}""", input=row.path)
                    luigi.File(filtered).copy(self.output().path)
                    break
            else:
                raise RuntimeError('no file found')

    def output(self):
        return luigi.LocalTarget(path=self.path(ext=self.format))

class DBPTripleMelange(DBPTask):
    """ Combine several slices of triples from DBP for KG. """
    version = luigi.Parameter(default="2014")

    def requires(self):
        return {
            'images-en': DBPImages(language='en', version=self.version),
            'images-de': DBPImages(language='de', version=self.version),
            'categories-en': DBPCategories(language='en', version=self.version),
            'categories-de': DBPCategories(language='de', version=self.version),
            'infobox-en': DBPInfobox(language='en', version=self.version),
            'infobox-de': DBPInfobox(language='de', version=self.version),
            'abstracts-de': DBPAbstracts(language='de', version=self.version),
            'links': DBPInterlanguageBacklinks(language='de', version=self.version),
        }

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for _, target in self.input().iteritems():
            shellout("cat {input} >> {output}", input=target.path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='nt'))

class DBPCount(DBPTask):
    """ Just count the number of triples in the mix and store it. """
    version = luigi.Parameter(default="2014")

    def requires(self):
        return DBPTripleMelange(version=self.version)

    @timed
    def run(self):
        output = shellout("wc -l {input} | awk '{{print $1}}' > {output}", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='count'))

class DBPTriplesSplitted(DBPTask):
    """ Split mixed Ntriples into chunks, so we see some progress in virtuoso. """

    version = luigi.Parameter(default="2014")
    lines = luigi.IntParameter(default=1000000)

    def requires(self):
        return DBPTripleMelange(version=self.version)

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

    version = luigi.Parameter(default="2014")
    graph = luigi.Parameter(default='http://dbpedia.org/resource/')
    host = luigi.Parameter(default='localhost', significant=False)
    port = luigi.IntParameter(default=1111, significant=False)
    username = luigi.Parameter(default='dba', significant=False)
    password = luigi.Parameter(default='dba', significant=False)

    def requires(self):
        return DBPTriplesSplitted(version=self.version)

    @timed
    def run(self):
        with self.input().open() as handle:
            path = handle.iter_tsv(cols=('path',)).next().path
            dirname = os.path.dirname(path)
            filename = os.path.basename(path)
            prefix, _ = filename.split('-')

        print("""
            Note: If you want to reload everything, clear any previous graph first:

                log_enable(0);
                SPARQL CLEAR GRAPH <http://dbpedia.org/resource/>;

            This is a manual task for now. If you haven't already:

            Run `isql-vt` (V6) or `isql` (V7), then execute:

                LD_DIR('{dirname}', '{prefix}-*', '{graph}');

            Check the result via:

                SELECT * FROM DB.DBA.LOAD_LIST;

            When you are done adding all triples files to the load list, run:

                RDF_LOADER_RUN();

            See also:

            * http://docs.openlinksw.com/virtuoso/fn_log_enable.html
            * http://docs.openlinksw.com/virtuoso/sparqlextensions.html#rdfsparulexamples10

            Note that "Section 16.3.2.3.10. Example for Dropping graph"
            calls graphs with 600M triples very large RDF data collections.

        """.format(dirname=dirname, prefix=prefix, graph=self.graph))

    def complete(self):
        return False

class DBPPredicateDistribution(DBPTask):
    """ Just a uniq -c on the predicate 'column' """
    version = luigi.Parameter(default="2014")
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

    version = luigi.Parameter(default="2014")
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

    version = luigi.Parameter(default="2014")
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

    version = luigi.Parameter(default="2014")
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

    version = luigi.Parameter(default="2014")
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

    # version = luigi.Parameter(default="2014")
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

class DBPGNDLinks(DBPTask):
    """ Find all links from DBP to GND via dp.de:gnd """
    version = luigi.Parameter(default="2014")
    language = luigi.Parameter(default="de")

    def requires(self):
        return DBPExtract(version=self.version, language=self.language, format='nt')

    @timed
    def run(self):
        relation = {
            'de': '<http://de.dbpedia.org/property/gnd>',
            'en': '<http://dbpedia.org/property/gnd>'
        }
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                if row.path.endswith('infobox_properties_%s.nt' % (self.language)):
                    output = shellout("""LANG=C grep -F "{relation}" {input} |
                                         sed -e 's@{relation}@@g' |
                                         sed -e 's@\^\^.*@@g' |
                                         sed -e 's@"@@g' > {output}""",
                                      relation=relation.get(self.language, 'en'),
                                      input=row.path)
                    luigi.File(output).move(self.output().path)
                    break

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='nt'), format=TSV)

class DBPGNDValidity(DBPTask):
    """ Check, how many <http://de.dbpedia.org/property/gnd> are
    actually working. """

    version = luigi.Parameter(default="2014")
    language = luigi.Parameter(default="de")

    def requires(self):
        from siskin.sources.gnd import GNDList
        return {
            'dbp': DBPGNDLinks(version=self.version, language=self.language),
            'gnd': GNDList(),
        }

    @timed
    def run(self):
        gnds = set()
        with self.input().get('gnd').open() as handle:
            for row in handle.iter_tsv(cols=('gnd',)):
                gnds.add(row.gnd)

        with self.input().get('dbp').open() as handle:
            with self.output().open('w') as output:
                for line in handle:
                    parts = line.strip().split()
                    if not len(parts) == 2:
                        self.logger.warn('skipping broken data: %s' % line)
                        continue
                    uri, id = parts
                    if id in gnds:
                        output.write_tsv(uri, id, 'OK')
                    else:
                        output.write_tsv(uri, id, 'INVALID')

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class DBPGNDValidLinks(DBPTask):
    """ Only keep the valid links and abbreviate
        <http://de.dbpedia.org/resource/Landkreis_Birkenfeld>
    to
        dbp:Landkreis_Birkenfeld
    """

    version = luigi.Parameter(default="2014")
    language = luigi.Parameter(default="de")

    def requires(self):
        return DBPGNDValidity(version=self.version, language=self.language)

    def run(self):
        relation = {
            'de': '<http://de.dbpedia.org/resource/',
            'en': '<http://dbpedia.org/resource/'
        }

        output = shellout("""
            grep -v INVALID {input} |
            awk '{{print $1"\\t"$2}}' |
            sed -e 's@{relation}@dbp:@g' |
            sed -e 's@>@@g' > {output}
            """, input=self.input().path, relation=relation.get(self.language, 'en'))

        with luigi.File(output, format=TSV).open() as handle:
            with self.output().open('w') as output:
                for row in handle.iter_tsv(cols=('dbp', 'gnd')):
                    output.write_tsv(row.dbp.decode('unicode-escape').encode('utf-8'), 'gnd:%s' % row.gnd)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class DBPGNDLinkDB(DBPTask):
    """ Create a single sqlite3 database containing the links from GND to DBPedia """
    version = luigi.Parameter(default="2014")
    language = luigi.Parameter(default="de")

    def requires(self):
        return DBPGNDValidLinks(version=self.version, language=self.language)

    def run(self):
        output = shellout("tabtokv -f 1,2 -o {output} {input}", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='db'))

class GNDDBPLinkDB(DBPTask):
    """ Create a single sqlite3 database containing the links from GND to DBPedia. """
    version = luigi.Parameter(default="2014")
    language = luigi.Parameter(default="de")

    def requires(self):
        return DBPGNDValidLinks(version=self.version, language=self.language)

    def run(self):
        output = shellout("tabtokv -f 2,1 -o {output} {input}", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path('db'))

class DBPGNDOverlap(DBPTask):
    """ Compute overlap between DBP and GND. """

    version = luigi.Parameter(default="2014")
    language = luigi.Parameter(default="de")
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        from siskin.sources.gnd import GNDDBPediaLinks
        return {
            'gtod': GNDDBPediaLinks(date=self.date),
            'dtog': DBPGNDValidLinks(version=self.version, language=self.language),
        }

    def run(self):
        gnd = set()
        dbp = set()

        with self.input().get('gtod').open() as handle:
            for row in handle.iter_tsv(cols=('dbp', 'gnd')):
                gnd.add((row.dbp, row.gnd))

        with self.input().get('dtog').open() as handle:
            for row in handle.iter_tsv(cols=('dbp', 'gnd')):
                dbp.add((row.dbp, row.gnd))

        info = {
            'gnd': len(gnd),
            'dbp': len(dbp),
            'gnd & dbp': len(gnd.intersection(dbp)),
            'gnd | dbp': len(gnd.union(dbp)),
            'gnd - dbp': len(gnd.difference(dbp)),
            'dbp - gnd': len(dbp.difference(gnd)),
        }

        print(json.dumps(info, indent=4))

        with self.output().open('w') as output:
            for d, g in gnd.union(dbp):
                flag = 'BOTH'
                if not (d, g) in dbp:
                    flag = 'ONLY_GND'
                if not (d, g) in gnd:
                    flag = 'ONLY_DBP'
                output.write_tsv(d, g, flag)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
