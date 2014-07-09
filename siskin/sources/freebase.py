# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

"""
Freebase.
"""

from gluish.benchmark import timed
from gluish.common import Executable
from gluish.utils import shellout
from siskin.task import DefaultTask
from gluish.intervals import monthly
from gluish.benchmark import timed
import luigi

class FreebaseTask(DefaultTask):
    TAG = 'freebase'

class FreebasePublic(FreebaseTask):
    """ Download bucket descriptor. """
    date = luigi.DateParameter(default=monthly())

    def requires(self):
        return Executable(name='wget')

    @timed
    def run(self):
        url = "http://commondatastorage.googleapis.com/freebase-public/"
        output = shellout(""" wget -q --retry-connrefused {url} -O {output}""", url=url)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))

class FreebaseDump(FreebaseTask):
    """ Download the dump. """
    date = luigi.DateParameter(default=monthly())

    def requires(self):
        return Executable(name='wget')

    @timed
    def run(self):
        url = "http://commondatastorage.googleapis.com/freebase-public/rdf/freebase-rdf-latest.gz"
        output = shellout(""" wget -q --retry-connrefused {url} -O {output}""", url=url)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='gz'))

class FreebaseExtract(FreebaseTask):
    """ Extract the dump. 250G+ !"""
    date = luigi.DateParameter(default=monthly())

    def requires(self):
        return FreebaseDump(date=self.date)

    @timed
    def run(self):
        output = shellout(""" gunzip -c {input} > {output}""",
                          input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='nt'))

class FreebaseJson(FreebaseTask):
    """ Convert all Freebase ntriples to a single JSON file. """

    date = luigi.DateParameter(default=monthly())
    language = luigi.Parameter(default="en")

    def requires(self):
        return FreebaseExtract()

    @timed
    def run(self):
        output = shellout("nttoldj -l {language} -i -a {input} >> {output}",
                          input=self.input().path, language=self.language)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class FreebaseEquivalent(FreebaseTask):
    """ Extract all owl:sameAS and equivalent* relations from freebase.

    $ curl -sL rdf.freebase.com/ns/m.0bgd3z7
    @prefix key: <http://rdf.freebase.com/key/>.
    @prefix ns: <http://rdf.freebase.com/ns/>.
    @prefix owl: <http://www.w3.org/2002/07/owl#>.
    @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#>.

    ns:m.0bgd3z7
        ns:base.ontologies.ontology.equivalent_instances    ns:m.0gkkff0;
        ns:base.ontologies.ontology.equivalent_instances    ns:m.0gkkghs;
        ns:base.politeuri.predicate.namespace_1    ns:m.09zs39j;
        key:en    "http_www_w3_org_2002_07_owl_sameas";
        ns:type.object.key    ns:en.http_www_w3_org_2002_07_owl_sameas;
        ns:type.object.name    "http://www.w3.org/2002/07/owl#sameAs"@en;
        ns:type.object.type    ns:base.politeuri.predicate;
        ns:type.object.type    ns:common.topic;
        ns:type.object.type    ns:base.ontologies.topic;
        ns:type.object.type    ns:base.ontologies.ontology;
        ns:rdf:type    ns:common.topic;
        ns:rdf:type    ns:base.ontologies.ontology;
        ns:rdf:type    ns:base.politeuri.predicate;
        ns:rdf:type    ns:base.ontologies.topic;
        rdfs:label    "http://www.w3.org/2002/07/owl#sameAs"@en.

    """
    date = luigi.DateParameter(default=monthly())

    def requires(self):
        return FreebaseExtract(date=self.date)

    @timed
    def run(self):
        output = shellout('LC_ALL="C" grep -E "(equivalent|owl#sameAs)" {path} > {output}',
                 path=self.input().path, ignoremap={1: "Not found."})
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='n3'))
