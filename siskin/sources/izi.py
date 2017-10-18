# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301
# Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
#                   Tracy Hoffmann, <tracy.hoffmann@uni-leipzig.de>
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
IZI Task #7755
"""

import datetime

import luigi
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.task import DefaultTask


class IZITask(DefaultTask):
    """ Base task for IZI """
    TAG = '78'

class IZIIntermediateSchema(IZITask):
    """ Convert XML to intermediate schema via metafacture """
    date = ClosestDateParameter(default=datetime.date.today())
    iziFile = luigi.Parameter(default='/tmp/IZI_XML.xml', description='path izi datadump', significant=False)

    def run(self):
        output = shellout("""flux.sh {flux} in={iziFile} > {output}""",
                         flux=self.assets("78/78.flux"), iziFile=self.iziFile)
        luigi.LocalTarget(output).move(self.output().path)
    
    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class IZIFincSolr(IZITask):
    """
    Export to finc solr schema by using span-export.
    Tag with ISIL for FID and change record type.
    """
    format = luigi.Parameter(default='solr5vu3', description='export format')
    isil = luigi.Parameter(default='DE-15-FID', description='isil FID')
    date = ClosestDateParameter(default=datetime.date.today())
    iziFile = luigi.Parameter(default='/tmp/IZI_XML.xml', description='path izi datadump', significant=False)

    def requires(self):
        return {
            'file': IZIIntermediateSchema(date=self.date, iziFile=self.iziFile)
        }
    
    def run(self):
        output = shellout("""span-export -o {format} -with-fullrecord <(span-tag -c <(echo '{{"{isil}": {{"any": {{}}}}}}') {input}) > {output}""",
                          format=self.format, isil=self.isil, input=self.input().get('file').path)
        output = shellout(
            """cat {input} | sed 's/"recordtype":"ai"/"recordtype":"is"/g' > {output}""", input=output)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='fincsolr.ndj'))
