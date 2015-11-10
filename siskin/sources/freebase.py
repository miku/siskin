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

"""
Freebase.
"""

from siskin.benchmark import timed
from gluish.common import Executable
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
import datetime
import luigi
import tempfile

class FreebaseTask(DefaultTask):
    TAG = 'freebase'

    def closest(self):
        return monthly(date=self.date)

class FreebasePublic(FreebaseTask):
    """ Download bucket descriptor. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return Executable(name='wget')

    @timed
    def run(self):
        url = "http://commondatastorage.googleapis.com/freebase-public/"
        output = shellout(""" wget --retry-connrefused {url} -O {output}""", url=url)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))

class FreebaseDump(FreebaseTask):
    """ Download the dump. """
    date = ClosestDateParameter(default=datetime.date.today())

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
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return FreebaseDump(date=self.date)

    @timed
    def run(self):
        output = shellout(""" gunzip -c {input} > {output}""",
                          input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='nt'))

class FreebaseAbbreviatedNTriples(FreebaseTask):
    """ Convert all Freebase ntriples to abbreviated ones. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return FreebaseExtract()

    @timed
    def run(self):
        output = shellout("TMPDIR={tmpdir} ntto -i -a -r {rules} {input} > {output}",
                          tmpdir=tempfile.tempdir, input=self.input().path, rules=self.assets('RULES.txt'))
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='nt'))

class FreebaseJson(FreebaseTask):
    """ Convert all Freebase ntriples to a single JSON file. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return FreebaseExtract()

    @timed
    def run(self):
        output = shellout("TMPDIR={tmpdir} ntto -i -a -j -r {rules} {input} > {output}",
                          tmpdir=tempfile.tempdir, input=self.input().path, rules=self.assets('RULES.txt'))
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
        output = shellout('LC_ALL="C" LANG=C grep -E "(equivalent|owl#sameAs)" {path} > {output}',
                 path=self.input().path, ignoremap={1: "Not found."})
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='n3'))
