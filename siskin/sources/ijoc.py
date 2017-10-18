# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301,C0330

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

import datetime

import luigi
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.sources.amsl import AMSLFilterConfig
from siskin.task import DefaultTask


"""
IJOC, refs #7138, #11005.
"""


class IJOCTask(DefaultTask):
    """ Base task for International Journal of Communication """
    TAG = '87'

    def closest(self):
        return monthly(date=self.date)


class IJOCHarvest(IJOCTask):
    """
    Harvest.
    """
    endpoint = luigi.Parameter(
        default='http://ijoc.org/index.php/ijoc/oai', significant=False)
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        shellout("""metha-sync "{endpoint}" """, endpoint=self.endpoint)
        output = shellout(
            """metha-cat -root Records "{endpoint}" > {output}""", endpoint=self.endpoint)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))


class IJOCIntermediateSchema(IJOCTask):
    """
    Convert to intermediate schema via metafacture. Custom morphs and flux are kept in assets/87.
    Maps are kept in assets/maps
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return IJOCHarvest(date=self.date)

    def run(self):
        mapdir = 'file:///%s' % self.assets("maps/")
        output = shellout("""flux.sh {flux} in={input} MAP_DIR={mapdir} | pigz -c > {output}""",
                          flux=self.assets("87/87.flux"), mapdir=mapdir, input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))


class IJOCFincSolr(IJOCTask):
    """
    Export to finc solr schema by using span-export.
    Tag with ISIL for FID and change record type.
    """
    format = luigi.Parameter(default='solr5vu3', description='export format')
    isil = luigi.Parameter(default='DE-15-FID', description='isil FID')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'config': AMSLFilterConfig(date=self.date),
            'file': IJOCIntermediateSchema(date=self.date)
        }

    def run(self):
        output = shellout("""span-tag -c {config} <(unpigz -c {input}) | span-export -o {format} -with-fullrecord > {output}""",
                          config=self.input().get('config').path, input=self.input().get('file').path,
                          format=self.format)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='fincsolr.ndj'))
