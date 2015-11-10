# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103
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
GND-related tasks.
"""

from elasticsearch import helpers as eshelpers
from gluish.common import Executable
from gluish.format import TSV
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from luigi.contrib.esindex import CopyToIndex
from siskin.benchmark import timed
from siskin.common import Directory
from siskin.database import sqlitedb
from siskin.sources.dbpedia import DBPGNDLinks, DBPDepictions
from siskin.task import DefaultTask
from siskin.utils import ElasticsearchMixin, random_string, iterfiles
import BeautifulSoup
import collections
import cPickle as pickle
import datetime
import elasticsearch
import hashlib
import HTMLParser
import itertools
import json
import luigi
import os
import prettytable
import re
import requests
import shutil
import string
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
    format = luigi.Parameter(default='ttl', description='alternatively rdf')

    @timed
    def run(self):
        host = "datendienst.dnb.de"
        path = "/cgi-bin/mabit.pl"
        params = {
            "userID": "opendata",
            "pass": "opendata",
            "cmd": "fetch",
            "mabheft": "GND.%s.gz" % self.format,
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
    format = luigi.Parameter(default='ttl', description='alternatively rdf')

    def requires(self):
        return GNDDump(date=self.date, format=self.format)

    @timed
    def run(self):
        output = shellout("gunzip -c {input} > {output}", input=self.input().fn)
        luigi.File(output).move(self.output().fn)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ttl'))

class GNDCacheDB(GNDTask):
    """ Turn the dump into a (id, content) sqlite3 db.
    This artefact will be used by the cache server.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GNDExtract(date=self.date, format='rdf')

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        pattern = re.compile("""rdf:about="http://d-nb.info/gnd/([0-9X-]+)">""")

        with sqlitedb(stopover) as cursor:
            cursor.execute("""CREATE TABLE gnd (id text  PRIMARY KEY, content blob)""")
            cursor.execute("""CREATE INDEX IF NOT EXISTS idx_gnd_id ON gnd (id)""")

            with self.input().open() as handle:
                groups = itertools.groupby(handle, key=str.isspace)
                for i, (k, lines) in enumerate(groups):
                    if k:
                        continue
                    lines = map(string.strip, list(lines))
                    match = pattern.search(lines[0])
                    if match:
                        row = (match.group(1), '\n'.join(lines))
                        cursor.execute("INSERT INTO gnd VALUES (?, ?)", row)

        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='db'))

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

class GNDTypes(GNDTask):
    """ Extract all rdf types. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GNDNTriples(date=self.date)

    def run(self):
        output = shellout("""LANG=C grep -F 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' {input} > {output}""", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GNDFriends(GNDTask):
    """ Extract friendship relations. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GNDNTriples(date=self.date)

    def run(self):
        output = shellout("""LANG=C grep -F 'http://d-nb.info/standards/elementset/gnd#acquaintanceshipOrFriendship' {input} > {output}""", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GNDFriendshipGraph(GNDTask):
    """ Create a simple DOT graph representation of the friendships. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GNDFriends(date=self.date)

    def run(self):
        with self.input().open() as handle:
            with self.output().open('w') as output:
                output.write("graph friends {\n")
                for line in handle:
                    parts = line.split()
                    if not len(parts) == 4:
                        continue
                    s, _, p, _ = parts
                    s = s.replace("http://d-nb.info/gnd/", "").strip("<>")
                    p = p.replace("http://d-nb.info/gnd/", "").strip("<>")
                    output.write('\t"%s" -- "%s";\n' % (s, p))
                output.write("}\n")

    def output(self):
        return luigi.LocalTarget(path=self.path())

class GNDFriendshipImage(GNDTask):
    """ Create an image of the graph. """
    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='svg')

    def requires(self):
        return GNDFriendshipGraph(date=self.date)

    @timed
    def run(self):
        output = shellout("dot -T{format} -o {output} {input}", format=self.format, input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext=self.format))

class GNDDates(GNDTask):
    """ Extract all dateOf... links. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GNDNTriples(date=self.date)

    def run(self):
        output = shellout("""LANG=C grep -F 'http://d-nb.info/standards/elementset/gnd#dateOf' {input} > {output}""", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GNDBiographicalData(GNDTask):
    """ For each GND collect birth date and death dates. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GNDDates(date=self.date)

    def run(self):
        biomap = collections.defaultdict(dict)
        with self.input().open() as handle:
            for line in handle:
                line = line.strip()
                parts = line.split()
                if len(parts) < 4:
                    raise RuntimeError('invalid ntriple: %s' % line)
                s, p, o = parts[0], parts[1], ' '.join(parts[2:-1])
                gnd = s.split('/')[-1].strip('<>')
                value = 'NOT_AVAILABLE'
                if o.endswith('^^<http://www.w3.org/2001/XMLSchema#date>'):
                    try:
                        value = datetime.date(*map(int, o[1:11].split('-')))
                    except ValueError as err:
                        self.logger.debug(line)
                        self.logger.warn(err)
                        continue

                elif o.endswith('^^<http://www.w3.org/2001/XMLSchema#gYear>'):
                    try:
                        value = datetime.date(int(o[1:5]), 1, 1)
                    except ValueError as err:
                        self.logger.debug(line)
                        self.logger.warn(err)
                        continue

                elif len(o.strip('"')) == 4:
                    if o.strip('"').isdigit():
                        value = datetime.date(int(o.strip('"')), 1, 1)
                else:
                    pass
                    # self.logger.debug('unknown type: %s' % line)

                if p == '<http://d-nb.info/standards/elementset/gnd#dateOfBirth>':
                    biomap[gnd]['b'] = value
                if p == '<http://d-nb.info/standards/elementset/gnd#dateOfDeath>':
                    biomap[gnd]['d'] = value

        with self.output().open('w') as output:
            for gnd, bio in biomap.iteritems():
                output.write_tsv(gnd, bio.get('b', 'NOT_AVAILABLE'), bio.get('d', 'NOT_AVAILABLE'))

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GNDContemporaryPeople(GNDTask):
    """ Build equivalence classes of people with birthdates within a given timespan. """
    date = ClosestDateParameter(default=datetime.date.today())
    threshold = luigi.FloatParameter(default=0.7)

    def requires(self):
        return GNDBiographicalData(date=self.date)

    @timed
    def run(self):
        yearspan = collections.defaultdict(set)
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('gnd', 'birth', 'death')):
                try:
                    birth = datetime.date(*map(int, row.birth.split('-')))
                    death = datetime.date(*map(int, row.death.split('-')))
                except ValueError:
                    continue

                yearspan[row.gnd] = set(range(birth.year, death.year + 1))

        with self.output().open('w') as output:
            for gnd, span in yearspan.iteritems():
                for other, o in yearspan.iteritems():
                    union = span.union(o)
                    if len(union) == 0:
                        continue
                    jaccard = len(span.intersection(o)) / float(len(union))
                    if jaccard > self.threshold:
                        output.write_tsv(gnd, other, jaccard)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GNDFoafPages(GNDTask):
    """ Extract all FOAF links. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GNDNTriples(date=self.date)

    def run(self):
        output = shellout("""LANG=C grep -F 'http://xmlns.com/foaf/0.1/page' {input} > {output}""", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GNDFoafLinks(GNDTask):
    """ Rewrite the GNDFoafPages output, so that we can compare it to
    the output of `DBPGNDValidLinks`. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GNDFoafPages(date=self.date)

    def run(self):
        output = shellout("""cut -d ' ' -f1,3 {input} |
                             sed -e 's@http://d-nb.info/gnd/@gnd:@g; s@http://de.wikipedia.org/wiki/@dbp:@g;' |
                             tr -d '<>' | awk '{{print $2" "$1}}' | tr ' ' '\\t' > {output}""", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GNDFoafOnly(GNDTask):
    """ Which links to dbpedia in GND are only via foaf:page? """

    date = ClosestDateParameter(default=datetime.date.today())
    version = luigi.Parameter(default="2014")
    language = luigi.Parameter(default="de")

    def requires(self):
        from siskin.sources.dbpedia import DBPGNDValidLinks
        return {'sa': DBPGNDValidLinks(version=self.version, language=self.language),
                'foaf': GNDFoafLinks(date=self.date)}

    def run(self):
        sa = set()
        with self.input().get('sa').open() as handle:
            for row in handle.iter_tsv(cols=('dbp', 'gnd')):
                sa.add((row.dbp, row.gnd))

        foaf = set()
        with self.input().get('foaf').open() as handle:
            for row in handle.iter_tsv(cols=('dbp', 'gnd')):
                foaf.add((row.dbp, row.gnd))

        with self.output().open('w') as output:
            for dbp, gnd in foaf.difference(sa):
                output.write_tsv(dbp, gnd)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GNDGeonames(GNDTask):
    """ Extract all geonames from dump """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GNDNTriples(date=self.date)

    def run(self):
        output = shellout("""LANG=C grep -F 'http://sws.geonames.org/' {input} |
                             cut -d ' ' -f3 | tr -d '<>' | sort -u > {output}""", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GNDList(GNDTask):
    """ Just dump a list of uniq GNDs. No URIs, just one id per line. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GNDNTriples(date=self.date)

    @timed
    def run(self):
        output = shellout("""LANG=C awk '{{print $1}}' {input} |
                             LANG=C sed -e 's@<http://d-nb.info/gnd/@@g' |
                             LANG=C sed -e 's@>@@g' |
                             LANG=C grep -v '^_:' | sort -u > {output}""", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GNDNames(GNDTask):
    """ Extract all preferred and variant names and list them as (gnd, name) tuples. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GNDNTriples(date=self.date)

    @timed
    def run(self):
        output = shellout("""LANG=C grep -E "variantName|preferredName" {input} |
                             LANG=C sed -e 's@<http://d-nb.info/gnd/\([0-9X-]*\)>@\\1@g' |
                             LANG=C sed -e 's@<http://d-nb.info/standards/elementset/gnd#[^>]*>@@g' |
                             LANG=C sed 's/.$//' | sed -e 's/"//g' | LANG=C grep -v "_:node" > {output}""", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GNDNamesWikipediaTitles(GNDTask):
    """ Match GND names with wikipedia page titles to find more connections. """
    date = ClosestDateParameter(default=datetime.date.today())
    language = luigi.Parameter(default='de')

    def requires(self):
        from siskin.sources.wikipedia import WikipediaTitles
        return {'gnd': GNDNames(date=self.date),
                'titles': WikipediaTitles(language=self.language)}

    @timed
    def run(self):
        titles = set()
        with self.input().get('titles').open() as handle:
            for row in handle.iter_tsv(cols=('name',)):
                titles.add(row.name)

        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().get('gnd').open() as handle:
            with luigi.File(stopover, format=TSV).open('w') as output:
                for line in handle:
                    parts = line.split()
                    if len(parts) == 0:
                        continue
                    gnd, name = parts[0], string.strip(' '.join(parts[1:]))
                    output.write_tsv('HIT' if name in titles else 'MISS', gnd, name)

        output = shellout("sort -u {input} > {output}", input=stopover)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

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
    """ Load GND parts into virtuoso 6.
    Clear any graph manually with `SPARQL CLEAR GRAPH <http://dbpedia.org/resource/>;`

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
            prefix, _ = filename.split('-')

        print("""
            Note: If you want to reload everything, clear any previous graph first:

                log_enable(3,1);
                SPARQL CLEAR GRAPH <{graph}>;

            -----------------------------------------------------

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
        with luigi.File(output, format=TSV).open() as handle:
            with self.output().open('w') as output:
                for row in handle.iter_tsv(cols=('gnd', 'dbp')):
                    decoded = urllib.unquote(row.dbp)
                    output.write_tsv(decoded, row.gnd)

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
            with sqlitedb(kv) as cursor:
                with self.output().open('w') as output:
                    for row in handle.iter_tsv(cols=('dbp', 'gnd')):
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
        with sqlitedb(stopover) as cursor:
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
