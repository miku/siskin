# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301
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

from siskin.benchmark import timed
from gluish.common import Executable
from luigi.contrib.esindex import CopyToIndex
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.sources.dbpedia import DBPAbbreviatedNTriples, DBPVirtuoso
from siskin.sources.gnd import GNDAbbreviatedNTriples, GNDVirtuoso
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
class GraphVirtuoso(GraphTask, luigi.WrapperTask):
    """
    Load GND and selected DBPedia triples into Virtuoso.
    """
    host = luigi.Parameter(default='localhost', significant=False)
    port = luigi.IntParameter(default=1111, significant=False)
    username = luigi.Parameter(default='dba', significant=False)
    password = luigi.Parameter(default='dba', significant=False)

    gnd = luigi.Parameter(default='http://d-nb.info/gnd/')
    date = ClosestDateParameter(default=datetime.date.today())

    dbpedia = luigi.Parameter(default='http://dbpedia.org/resource/')
    version = luigi.Parameter(default="3.9")
    base = luigi.Parameter(default='http://downloads.dbpedia.org',
                           description='base url, for 2014 version, try http://data.dws.informatik.uni-mannheim.de/dbpedia',
                           significant=False)

    def requires(self):
        return {'dbpedia': DBPVirtuoso(host=self.host, port=self.port, username=self.username, password=self.password,
                                       graph=self.dbpedia, version=self.version, base=self.base),
                'gnd': GNDVirtuoso(host=self.host, port=self.port, username=self.username, password=self.password,
                                   graph=self.gnd, date=self.date)}

    def output(self):
        return self.input()
