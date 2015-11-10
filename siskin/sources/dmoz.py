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
Formelly known as directory.mozilla.org.

DMOZ uses a hierarchical ontology scheme for organizing site listings.
Listings on a similar topic are grouped into categories
which can then include smaller categories.
"""

from siskin.benchmark import timed
from gluish.common import Executable
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
import datetime
import luigi

class DMOZTask(DefaultTask):
    """ DMOZ supertask. """
    TAG = 'dmoz'

    def closest(self):
        return monthly(date=self.date)

class DMOZStructureDownload(DMOZTask):
    """ Download structure RDF. """
    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default='http://rdf.dmoz.org/rdf/structure.rdf.u8.gz', significant=False)

    def requires(self):
        return [Executable(name='curl'), Executable(name='gunzip')]

    @timed
    def run(self):
        output = shellout("""curl "{url}" | gunzip -c > {output}""", url=self.url)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='rdf'))

class DMOZContentDownload(DMOZTask):
    """ Download content RDF. """
    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default='http://rdf.dmoz.org/rdf/content.rdf.u8.gz', significant=False)

    def requires(self):
        return [Executable(name='curl'), Executable(name='gunzip')]

    @timed
    def run(self):
        output = shellout("""curl "{url}" | gunzip -c > {output}""", url=self.url)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='rdf'))
