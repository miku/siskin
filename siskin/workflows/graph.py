# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

from gluish.benchmark import timed
from gluish.common import Executable
from gluish.esindex import CopyToIndex
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.sources.dbpedia import DBPAbbreviatedNTriples, DBPImages, DBPInterlanguageBacklinks, DBPCategories
from siskin.sources.gnd import GNDAbbreviatedNTriples, GNDNTriples
from siskin.task import DefaultTask
import datetime
import luigi
import shutil
import tempfile

class GraphTask(DefaultTask):
    TAG = 'graph'

    def closest(self):
        return monthly(date=self.date)

class GraphCombinedNTriples(GraphTask):
    """ Combine German and English DBpedia and GND. """
    date = ClosestDateParameter(default=datetime.date.today())
    version = luigi.Parameter(default="3.9")

    def requires(self):
        return {'gnd': GNDAbbreviatedNTriples(date=self.date),
                'de': DBPAbbreviatedNTriples(version=self.version, language='de'),
                'en': DBPAbbreviatedNTriples(version=self.version, language='en')}

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for _, target in self.input().iteritems():
            shellout("cat {input} >> {output}", input=target.path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='nt'))

class GraphCombinedJson(GraphTask):
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'ntriples': GraphCombinedNTriples(date=self.date),
                'ntto': Executable(name='ntto')}

    @timed
    def run(self):
        output = shellout("ntto -i -j {input} > {output}", input=self.input().get('ntriples').path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class GraphIndex(GraphTask, CopyToIndex):
    date = ClosestDateParameter(default=datetime.date.today())

    index = 'graph'
    doc_type = 'triple'
    purge_existing_index = True
    mapping = {'triple': {'date_detection': False,
                          'properties': {
                            's': {'type': 'string', 'index': 'not_analyzed'},
                            'p': {'type': 'string', 'index': 'not_analyzed'},
                            'o': {'type': 'string', 'index': 'not_analyzed'}}}}

    def requires(self):
        return GraphCombinedJson(date=self.date)

class GraphCayleyLevelDB(GraphTask):
    """ Create a Cayley LevelDB database from GND and dbpedia data. """
    date = ClosestDateParameter(default=datetime.date.today())
    version = luigi.Parameter(default="3.9")
    language = luigi.Parameter(default="de")

    gomaxprocs = luigi.IntParameter(default=8, significant=False)

    def requires(self):
        return {'ntriples': GraphCombinedNTriples(date=self.date),
                'cayley': Executable(name='cayley', message='http://git.io/KH-wFA')}

    @timed
    def run(self):
        dbpath = tempfile.mkdtemp(prefix='siskin-')
        shellout("cayley init -alsologtostderr -config {config} -dbpath={dbpath}",
             config=self.assets('cayley.leveldb.conf'), dbpath=dbpath)
        shellout("GOMAXPROCS={gomaxprocs} cayley load -config {config} -alsologtostderr -dbpath={dbpath} --triples {input}",
             gomaxprocs=self.gomaxprocs, config=self.assets('cayley.leveldb.conf'), dbpath=dbpath, input=self.input().get('ntriples').path)
        shutil.move(dbpath, self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='leveldb'))

class GraphCayleyMongoDB(GraphTask):
    """ Create a Cayley MongoDB database from GND and dbpedia data. """
    date = ClosestDateParameter(default=datetime.date.today())
    version = luigi.Parameter(default="3.9")
    language = luigi.Parameter(default="de")

    gomaxprocs = luigi.IntParameter(default=8, significant=False)

    def requires(self):
        return {'ntriples': GraphCombinedNTriples(date=self.date),
                'cayley': Executable(name='cayley', message='http://git.io/KH-wFA')}

    @timed
    def run(self):
        shellout("cayley init -alsologtostderr -config {config}",
                 config=self.assets('cayley.mongodb.conf'))
        shellout("cayley load -config {config} -alsologtostderr --triples {input}",
                 config=self.assets('cayley.mongodb.conf'),
                 input=self.input().get('ntriples').path)
        shellout("touch {output}", output=self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mongo'))

#
# Virtuoso related tasks
#

class GraphTripleMelange(GraphTask):
    """ Combine several slices of triples for KG. """

    date = ClosestDateParameter(default=datetime.date.today())
    version = luigi.Parameter(default="3.9")

    def requires(self):
        return {
            'gnd': GNDNTriples(date=self.date),
            'images-en': DBPImages(language='en', version=self.version),
            'images-de': DBPImages(language='de', version=self.version),
            'categories-en': DBPCategories(language='en', version=self.version),
            'categories-de': DBPCategories(language='de', version=self.version),
            'links': DBPInterlanguageBacklinks(language='de', version=self.version),
        }

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for _, target in self.input().iteritems():
            shellout("cat {input} >> {output}", input=target.path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='nt'))

class GraphTriplesSplitted(GraphTask):
    """ Split Ntriples into chunks, so we see some progress in virtuoso. """

    date = ClosestDateParameter(default=datetime.date.today())
    version = luigi.Parameter(default="3.9")
    lines = luigi.IntParameter(default=1000000)

    def requires(self):
        return GraphTripleMelange(date=self.date, version=self.version)

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

class GraphVirtuoso(GraphTask):
    """ Load GND into virtuoso 6. Assume there is no graph yet.
    Clear any graph manually with `SPARQL CLEAR GRAPH <http://d-nb.info/gnd/>;`

    See SELECT * FROM DB.DBA.LOAD_LIST; for progress.

    Also add `dirname $(taskoutput GNDNTriplesSplitted)` to
    AllowedDirs in /etc/virtuoso-opensource-6.1/virtuoso.ini
    """

    date = ClosestDateParameter(default=datetime.date.today())
    graph = luigi.Parameter(default='http://d-nb.info/gnd/')
    host = luigi.Parameter(default='localhost', significant=False)
    port = luigi.IntParameter(default=1111, significant=False)
    username = luigi.Parameter(default='dba', significant=False)
    password = luigi.Parameter(default='dba', significant=False)

    def requires(self):
        return GNDNTriplesSplitted(date=self.date)

    @timed
    def run(self):
        with self.input().open() as handle:
            path = handle.iter_tsv(cols=('path',)).next().path
            dirname = os.path.dirname(path)
            filename = os.path.basename(path)
            prefix, id = filename.split('-')

        # allow access to all to dirname
        os.chmod(dirname, 0777)

        cmd = """
            LD_DIR ('%s', '%s-*', '%s');
            RDF_LOADER_RUN();
        """ % (dirname, prefix, self.graph)
        _, tmp = tempfile.mkstemp(prefix='siskin')
        with open(tmp, 'w') as output:
            output.write(cmd)
        shellout("isql-vt {host}:{port} {username} {password} {file}",
                 host=self.host, port=self.port, username=self.username, password=self.password, file=tmp)
        with self.output().open('w') as output:
            pass

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='touch', digest=True))
