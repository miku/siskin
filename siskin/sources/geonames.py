# coding: utf-8

from gluish.benchmark import timed
from gluish.common import Executable
from gluish.intervals import quarterly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout, nwise
from siskin.sources.gnd import GNDGeonames
from siskin.task import DefaultTask
import datetime
import luigi
import tempfile

class GeonamesTask(DefaultTask):
    TAG = 'geonames'

    def closest(self):
        return quarterly(date=self.date)

class GeonamesDump(GeonamesTask):
    """ Download dump. zipfile assumed, with a single file called
    all-geonames-rdf.txt inside. """

    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("wget --retry-connrefused -O {output} http://download.geonames.org/all-geonames-rdf.zip")
        output = shellout("unzip -c {zipfile} 'all-geonames-rdf.txt' > {output}", zipfile=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

class GeonamesGND(GeonamesTask):
    """ For each reference in GND extract the RDF from GeonamesDump
    and convert it to ntriples with rapper.

    Inputs: A geonames dump with alternating URI and RDF/XML lines,
    a list of geonames.org URIs, as they appear in the GND.

    $ head -2 $(taskoutput GeonamesDump)
    http://sws.geonames.org/1/
    <?xml version="1.0" encoding="UTF-8" standalone="no"?>
    <rdf:RDF xmlns:cc="http://creativecommons.org/ns#" ...

    $ head -2 $(taskoutput GNDGeonames)
    http://sws.geonames.org/1000501
    http://sws.geonames.org/1007311
    ...

    Output is a single ntriples file that contains all facts about locations
    referenced by GND. Example:

    $ head -3 $(taskoutput GeonamesGND)

    <http://sws.geonames.org/49518/> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.geonames.org/ontology#Feature> .
    <http://sws.geonames.org/49518/> <http://www.w3.org/2000/01/rdf-schema#isDefinedBy> "http://sws.geonames.org/49518/about.rdf" .
    <http://sws.geonames.org/49518/> <http://www.geonames.org/ontology#name> "Republic of Rwanda" .
    ...
    Takes about 5 minutes. Yields 1012541 triples about 42429 locations (2014-11).
    """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'geo': GeonamesDump(date=self.date),
                'gnd': GNDGeonames(date=self.date),
                'rapper': Executable(name='rapper', message='http://librdf.org/raptor/rapper.html')}

    @timed
    def run(self):
        ids = set()
        with self.input().get('gnd').open() as handle:
            for row in handle.iter_tsv(cols=('uri',)):
                ids.add(row.uri.rstrip('/'))

        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().get('geo').open() as handle:
            with luigi.File(stopover).open('w') as output:
                while True:
                    try:
                        line = handle.next().strip()
                        if line.startswith('http://'):
                            content = handle.next()
                            if line.rstrip('/') in ids:
                                output.write(content)
                    except StopIteration:
                        break

        _, t = tempfile.mkstemp(prefix='siskin-')
        output = shellout("""while read r; do echo $r > {t} &&
                             rapper -q -i rdfxml -o ntriples {t} >> {output}; done < {input} """,
                             t=t, input=stopover)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='nt'))

class GeonamesSeeAlso(GeonamesTask):
    """ Extract rdfs:seeAlso and remove slashes from geonames subject,
    since GND use URIs without slashes. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GeonamesGND(date=self.date)

    @timed
    def run(self):
        output = shellout("""LANG=C grep -F "http://www.w3.org/2000/01/rdf-schema#seeAlso" {input} | sed -e 's@/>@>@g' > {output} """, input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='nt'))
