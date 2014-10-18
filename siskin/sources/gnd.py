# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103

"""
GND-related tasks.
"""

from elasticsearch import helpers as eshelpers
from gluish.benchmark import timed
from gluish.common import ElasticsearchMixin, Executable, Directory
from gluish.database import sqlite3db
from gluish.esindex import CopyToIndex
from gluish.format import TSV
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.path import iterfiles
from gluish.utils import shellout, random_string
from siskin.sources.dbpedia import DBPGNDLinks, DBPDepictions
from siskin.task import DefaultTask
import BeautifulSoup
import collections
import cPickle as pickle
import datetime
import elasticsearch
import hashlib
import HTMLParser
import json
import luigi
import os
import prettytable
import requests
import shutil
import tempfile
import urllib

class GNDTask(DefaultTask):
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
            raise RuntimeError('No tabular data found on {0}'.format(r.url))

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
        luigi.File(output).move(self.output().path)

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

class GNDCount(GNDTask):
    """ Just count the number of triples and store it. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GNDNTriples(date=self.date)

    @timed
    def run(self):
        output = shellout("wc -l {input} | awk '{{print $1}}' > {output}", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='count'))

class GNDNTriplesSplitted(GNDTask):
    """ Split Ntriples into chunks, so we see some progress in virtuoso. """

    date = ClosestDateParameter(default=datetime.date.today())
    lines = luigi.IntParameter(default=1000000)

    def requires(self):
        return GNDNTriples(date=self.date)

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

class GNDVirtuoso(GNDTask):
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

        task = GNDCount(date=self.date)
        luigi.build([task], local_scheduler=True)
        with task.output().open() as handle:
            expected = int(handle.read().strip())
            if expected == 0:
                return False
            elif expected == loaded:
                return True
            else:
                raise RuntimeError('expected %s triples but loaded %s' % (expected, loaded))

class GNDAbbreviatedNTriples(GNDTask):
    """ Get a Ntriples representation of GND, but abbreviate with ntto. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GNDNTriples(date=self.date)

    @timed
    def run(self):
        output = shellout("ntto -a -r {rules} -o {output} {input}", input=self.input().path, rules=self.assets('RULES.txt'))
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='nt'))

class GNDPredicateDistribution(GNDTask):
    """ Just a uniq -c on the predicate 'column' """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GNDNTriples(date=self.date)

    def run(self):
        output = shellout("""cut -d " " -f2 {input} | LANG=C sort | LANG=C uniq -c > {output}""",
                          input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='txt'))

class GNDCayleyLevelDB(GNDTask):
    """ Create a Cayley LevelDB database from GND data. """
    date = ClosestDateParameter(default=datetime.date.today())
    gomaxprocs = luigi.IntParameter(default=8, significant=False)

    def requires(self):
        return {'ntriples': GNDAbbreviatedNTriples(date=self.date),
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

class GNDCayleyBoltDB(GNDTask):
    """ Create a Cayley BoltDB database from GND data. """
    date = ClosestDateParameter(default=datetime.date.today())
    gomaxprocs = luigi.IntParameter(default=8, significant=False)

    def requires(self):
        return {'ntriples': GNDAbbreviatedNTriples(date=self.date),
                'cayley': Executable(name='cayley', message='http://git.io/KH-wFA')}

    @timed
    def run(self):
        _, dbpath = tempfile.mkstemp(prefix='siskin-')
        shellout("cayley init -alsologtostderr -config {config} -dbpath={dbpath}",
                 config=self.assets('cayley.bolt.conf'), dbpath=dbpath)
        shellout("GOMAXPROCS={gomaxprocs} cayley load -config {config} -alsologtostderr -dbpath={dbpath} --triples {input}",
                 gomaxprocs=self.gomaxprocs, config=self.assets('cayley.bolt.conf'), dbpath=dbpath, input=self.input().get('ntriples').path)
        shutil.move(dbpath, self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='bolt'))

class GNDCayleyMongoDB(GNDTask):
    """ Create a Cayley MongoDB database from GND and dbpedia data. """
    date = ClosestDateParameter(default=datetime.date.today())
    gomaxprocs = luigi.IntParameter(default=8, significant=False)

    def requires(self):
        return {'ntriples': GNDAbbreviatedNTriples(date=self.date),
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

class GNDJson(GNDTask):
    """ Convert GND to some indexable JSON. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GNDNTriples(date=self.date)

    @timed
    def run(self):
        output = shellout("ntto -r {rules} -a -j -i {input} > {output}", input=self.input().path, rules=self.assets('RULES.txt'))
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

class GNDTaxonomy(GNDTask):
    """
    For each term in broaderTermGeneral, save broader and narrower terms.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GNDRelations(date=self.date, relation='dnb:broaderTermGeneral')

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
                    if value not in taxonomy:
                        break
                    else:
                        value = taxonomy[value]
                        if value in broader:
                            break
                output.write_tsv(key, "|".join(broader))

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GNDDBPediaLinks(GNDTask):
    """ Return (s, o) tuples that connect gnd and wikipedia. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GNDRelations(date=self.date, relation='owl:sameAs')

    def run(self):
        output = shellout(r""" awk '$2 ~ "dbp:" {{print $0}}' {input} > {output}""", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GNDDBPediaInterlinks(GNDTask):
    """
    Report GND <-> dbpedia interlinking stats.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'gndto': GNDDBPediaLinks(date=self.date),
            'dbpto': DBPGNDLinks(),
            'gndpage': GNDPage(date=self.date, relation='foaf:Page')}

    def run(self):
        gndto = set()
        with self.input().get('gndto').open() as handle:
            for row in handle.iter_tsv(cols=('gnd', 'dbp')):
                gndto.add(row.gnd)

        dbpto = set()
        with self.input().get('dbpto').open() as handle:
            for row in handle.iter_tsv(cols=('gnd', 'p', 'dbp')):
                dbpto.add(row.gnd)

        gndpage = set()
        with self.input().get('gndpage').open() as handle:
            for row in handle.iter_tsv(cols=('gnd', 'url')):
                dbpto.add(row.gnd)

        result = dict(
            gnd_links=len(gndto),
            dbp_links=len(dbpto),
            gnd_and_dbp=len(gndto.intersection(dbpto)),
            gnd_only=len(gndto - dbpto),
            dbp_only=len(dbpto - gndto),
            gnd_or_dbp=len(gndto.union(dbpto)),
            dbp_and_via_url=len(dbpto.intersection(gndpage)),
        )
        with self.output().open('w') as output:
            for key, value in result.iteritems():
                output.write_tsv(key, value)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GNDDepictions(GNDTask):
    """
    For each GND, that has a DBP link (TODO: more is possible via backlinks)
    report the image URL in foaf:depiction.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'gndto': GNDDBPediaLinks(date=self.date),
                'dbppics': DBPDepictions()}

    @timed
    def run(self):
        kv = shellout(""" tabtokv -f "1,3" -o {output} {input}""", input=self.input().get('dbppics').path)
        with self.input().get('gndto').open() as handle:
            with sqlite3db(kv) as cursor:
                with self.output().open('w') as output:
                    for row in handle.iter_tsv(cols=('gnd', 'dbp')):
                        cursor.execute("""select value from store where key = ?""", (row.dbp,))
                        result = cursor.fetchall()
                        for url in set(result):
                            output.write_tsv(row.gnd, url[0])

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GNDDownloadDepictions(GNDTask):
    """ Download depictions from wikimedia, that are linked with a GND, about 70k. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'urls': GNDDepictions(date=self.date), 'dir': Directory(path=self.taskdir())}

    def run(self):
        with self.input().get('urls').open() as handle:
            for i, row in enumerate(handle.iter_tsv(cols=('gnd', 'url'))):
                gnd = row.gnd.replace('gnd:', '')
                _, ext = os.path.splitext(row.url)
                imgname = '%s-%s%s' % (gnd, hashlib.sha1(row.url).hexdigest(), ext)
                imgpath = os.path.join(self.taskdir(), imgname)
                if not os.path.exists(imgpath):
                    output = shellout("""wget -q -O {output} "{url}" """, output=imgpath, url=row.url, ignoremap={8: '404s throw 8'})

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GNDDepictionsDB(GNDTask):
    """ Create a small KV store. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GNDDepictions(date=self.date)

    def run(self):
        output = shellout(""" tabtokv -f "1,2" -o {output} {input} """, input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='db'))

class GNDPredicates(GNDTask):
    """ Report all existing predicates. """
    date = ClosestDateParameter(default=datetime.date.today())
    index = luigi.Parameter(default='gnd', description='name of the index to search')

    def requires(self):
        return Executable(name='estab', message='http://git.io/bLY7cQ')

    @timed
    def run(self):
        output = shellout(r""" estab -indices {index} -f "p" | sort -u > {output} """, index=self.index)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True), format=TSV)

class GNDDatabase(GNDTask):
    """
    Build a relational version of GND. sqlite3, each relation
    gets an indexed (s, o) table.
    """

    date = ClosestDateParameter(default=datetime.date.today())
    index = luigi.Parameter(default='gnd', description='name of the index to search')

    def requires(self):
        relations = [
            "dnb:acquaintanceshipOrFriendship",
            "dnb:affiliation",
            "dnb:annotator",
            "dnb:architect",
            "dnb:broaderTermGeneral",
            "dnb:definition",
            "dnb:familialRelationship",
            "dnb:fieldOfActivity",
            "dnb:fieldOfStudy",
            "dnb:functionOrRole",
            "dnb:memberOfTheFamily",
            "dnb:placeOfActivity",
            "dnb:placeOfBirth",
            "dnb:placeOfExile",
            "dnb:playedInstrument",
            # "dnb:preferredNameForThePerson",
            "dnb:professionOrOccupation",
            "dnb:professionalRelationship",
            "dnb:relatedTerm",
            "dnb:related",
            # "foaf:Page",
            # "skos:narrowMatch",
        ]
        tasks = {}
        for relation in relations:
            tasks[relation] = GNDRelations(date=self.date, index=self.index, relation=relation)
        return tasks

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with sqlite3db(stopover) as cursor:
            for relation, target in self.input().iteritems():
                table = relation.replace(':', '_')
                cursor.execute("""CREATE TABLE IF NOT EXISTS %s (s TEXT, o TEXT)""" % table)
                with target.open() as handle:
                    for row in handle.iter_tsv(cols=('s', 'o')):
                        cursor.execute("""INSERT INTO %s (s, o) VALUES (?, ?)""" % table, row)
                cursor.connection.commit()
                cursor.execute("""CREATE INDEX IF NOT EXISTS idx_%s_s on %s (s)""" % (table, table))
                cursor.execute("""CREATE INDEX IF NOT EXISTS idx_%s_o on %s (o)""" % (table, table))
                cursor.execute("""CREATE INDEX IF NOT EXISTS idx_%s_s_o on %s (s, o)""" % (table, table))
                cursor.execute("""CREATE INDEX IF NOT EXISTS idx_%s_o_s on %s (o, s)""" % (table, table))
                cursor.connection.commit()
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='db'))

class GNDRelations(GNDTask):
    """
    Extract relations from GND.
    Given the predicate shortname, extract (s, o) tuples from the index.

    Example relations:

    * dnb:acquaintanceshipOrFriendship
    * dnb:affiliation
    * dnb:annotator
    * dnb:architect
    * dnb:broaderTermGeneral
    * dnb:definition
    * dnb:familialRelationship
    * dnb:fieldOfActivity
    * dnb:fieldOfStudy
    * dnb:functionOrRole
    * dnb:memberOfTheFamily
    * dnb:placeOfActivity
    * dnb:placeOfExile
    * dnb:playedInstrument
    * dnb:preferredNameForThePerson
    * dnb:relatedTerm
    * foaf:Page
    * skos:narrowMatch
    """

    date = ClosestDateParameter(default=datetime.date.today())
    relation = luigi.Parameter(default='dnb:definition')
    index = luigi.Parameter(default='gnd', description='name of the index to search')

    def requires(self):
        return Executable(name='estab', message='http://git.io/bLY7cQ')

    def run(self):
        output = shellout(r""" estab -indices {index} -f "s o"
            -query '{{"query": {{"query_string": {{"query": "p:\"{relation}\""}}}}}}' > {output}""", index=self.index, relation=self.relation)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True), format=TSV)

class GNDRelationsDB(GNDTask):
    """ Build an sqlite key-value database from a single relation in the GND. """
    date = ClosestDateParameter(default=datetime.date.today())
    relation = luigi.Parameter(default='dnb:definition')
    index = luigi.Parameter(default='gnd', description='name of the index to search')

    def requires(self):
        return GNDRelations(date=self.date, relation=self.relation, index=self.index)

    @timed
    def run(self):
        output = shellout(r""" tabtokv -f "1,2" -o {output} {input} """, input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True))
