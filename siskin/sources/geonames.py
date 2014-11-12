# coding: utf-8

from gluish.intervals import quarterly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout, nwise
from siskin.task import DefaultTask
from siskin.sources.gnd import GNDGeonames
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
    """ For each reference in GND extract the RDF from GeonamesDump"""

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'geo': GeonamesDump(date=self.date),
                'gnd': GNDGeonames(date=self.date)}

    def run(self):
        ids = set()
        with self.input().get('gnd').open() as handle:
            for row in handle.iter_tsv(cols=('uri',)):
                ids.add(row.uri.split('/')[-1])

        commands = []
        for batch in nwise(ids, n=5000):
            commands.append("""awk '/^http:\/\/sws.geonames.org\/(%s)\//{{getline; print}}'""" % '|'.join(batch))
        command = "%s %s | %s" % (commands[0], self.input().get('geo').path, ' | '.join(commands[1:]))
        output = shellout("""{command} > {output}""", command=command)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())
