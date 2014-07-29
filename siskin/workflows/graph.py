# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

from gluish.benchmark import timed
from gluish.common import Executable
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.sources.dbpedia import DBPAbbreviatedNTriples
from siskin.sources.gnd import GNDAbbreviatedNTriples
from siskin.task import DefaultTask
import datetime
import luigi
import shutil
import tempfile

class GraphTask(DefaultTask):
    TAG = 'graph'

class GraphCombineNTriples(GraphTask):
    date = ClosestDateParameter(default=datetime.date.today())
    version = luigi.Parameter(default="3.9")
    language = luigi.Parameter(default="de")

    def requires(self):
        return {'gnd': GNDAbbreviatedNTriples(date=self.date),
                'dbp': DBPAbbreviatedNTriples(version=self.version,
                                              language=self.language)}

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for _, target in self.input():
            shellout("cat {input} >> {output}", input=target.path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='nt'))

class GraphCayleyLevelDB(GraphTask):
    """ Create a Cayley LevelDB database from GND and dbpedia data. """
    date = ClosestDateParameter(default=datetime.date.today())
    version = luigi.Parameter(default="3.9")
    language = luigi.Parameter(default="de")

    gomaxprocs = luigi.IntParameter(default=8, significant=False)

    def requires(self):
        return {'ntriples': GraphCombineNTriples(date=self.date),
                'cayley': Executable(name='cayley', message='http://git.io/KH-wFA')}

    @timed
    def run(self):
        dbpath = tempfile.mkdtemp(prefix='siskin-')
        shellout("cayley init -alsologtostderr -config {config} -dbpath={dbpath}",
             config=self.assets('cayley.conf'), dbpath=dbpath)
        shellout("GOMAXPROCS={gomaxprocs} cayley load -config {config} -alsologtostderr -dbpath={dbpath} --triples {input}",
             gomaxprocs=self.gomaxprocs, config=self.assets('cayley.conf'), dbpath=dbpath, input=self.input().get('ntriples').path)
        shutil.move(dbpath, self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='leveldb'))
