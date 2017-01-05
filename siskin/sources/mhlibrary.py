# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301

# Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
#                   Martin Czygan, <martin.czygan@uni-leipzig.de>
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

"""
Example workflow with OAI harvest and metafacture.
"""

import datetime

import luigi

from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask


class MHLibraryTask(DefaultTask):
    """ Base taske for MH Library """
    TAG = '103'

    def closest(self):
        return monthly(date=self.date)


class MHLibraryHarvest(MHLibraryTask):
    """
    Harvest.
    """
    endpoint = luigi.Parameter(
        default='http://cdm15759.contentdm.oclc.org/oai/oai.php', significant=False)
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        shellout("""metha-sync "{endpoint}" """, endpoint=self.endpoint)
        output = shellout(
            """metha-cat -root Records "{endpoint}" > {output}""", endpoint=self.endpoint)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))


class MHLibraryIntermediateSchema(MHLibraryTask):
    """
    Convert to intermediate schema via metafacture. Custom morphs and flux are kept in assets/87.
    Maps are kept in assets/maps
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return MHLibraryHarvest(date=self.date)

    def run(self):
        mapdir = 'file:///%s' % self.assets("maps/")
        output = shellout("""flux.sh {flux} in={input} MAP_DIR={mapdir} > {output}""",
                          flux=self.assets("103/103.flux"), mapdir=mapdir, input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))


class MHLibraryFincSolr(MHLibraryTask):
    """
    Export to finc solr schema by using span-export.
    Tag with ISIL for FID and change record type.
    """
    format = luigi.Parameter(default='solr5vu3', description='export format')
    isil = luigi.Parameter(default='DE-15-FID', description='isil FID')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'file': MHLibraryIntermediateSchema(date=self.date)
        }

    def run(self):
        output = shellout("""span-export -o {format} <(span-tag -c <(echo '{{"{isil}": {{"any": {{}}}}}}') {input}) > {output}""",
                          format=self.format, isil=self.isil, input=self.input().get('file').path)
        output = shellout(
            """cat {input} | sed 's/"recordtype":"ai"/"recordtype":"is"/g' > {output}""", input=output)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ndj'))
