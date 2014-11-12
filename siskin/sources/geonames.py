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
    and convert it to ntriples with rapper. """

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
                ids.add('%s/' % row.uri)

        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().get('geo').open() as handle:
            with luigi.File(stopover).open('w') as output:
                while True:
                    try:
                        line = handle.next().strip()
                        if line.startswith('http://'):
                            if line in ids:
                                output.write(handle.next())
                            else:
                                handle.next()
                    except StopIteration:
                        break

        _, t = tempfile.mkstemp(prefix='siskin-')
        output = shellout("""while read r; do echo $r > {t} &&
                             rapper -q -i rdfxml -o ntriples {t} >> {output}; done < {input} """,
                             t=t, input=stopover)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='nt'))
