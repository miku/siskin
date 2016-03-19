# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

#  Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                    The Finc Authors, http://finc.info
#                    Martin Czygan, <martin.czygan@uni-leipzig.de>
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

from __future__ import print_function
from gluish.common import Executable
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from luigi.contrib.esindex import CopyToIndex
from siskin.benchmark import timed
from siskin.task import DefaultTask
import datetime
import luigi
import urllib

class RISMTask(DefaultTask):
    TAG = '014'

    def closest(self):
        return datetime.date(2014, 1, 1)

class RISMSync(RISMTask):
    """ Get at most n records (defaults to 20000). """
    date = ClosestDateParameter(default=datetime.date.today())
    maximum = luigi.IntParameter(default=20000, significant=False)

    def requires(self):
        return Executable(name='wget')

    @timed
    def run(self):
        base = 'http://service1.rism.info/opac'
        params = {
            'operation': 'searchRetrieve',
            'version': '1.1',
            'query': 'source=http*',
            'maximumRecords': self.maximum,
        }
        output = shellout("""wget --retry-connrefused -O {output} "{base}?{params}" """,
                          base=base, params=urllib.urlencode(params))
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))

class RISMMarc(RISMTask):
    """ Convert IMSLP XML (trans, antizebra) to MARCXML, then to MARC """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'file': RISMSync(date=self.date),
                'apps': [Executable(name='xsltproc'),
                         Executable(name='yaz-marcdump'),
                         Executable(name='marcuniq')]}

    @timed
    def run(self):
        # first trans.xsl
        output = shellout("xsltproc {stylesheet} {input} > {output}",
                          input=self.input().get('file').path,
                          stylesheet=self.assets('trans.xsl'))
        # then antizebra.xsl
        output = shellout("xsltproc {stylesheet} {input} > {output}",
                          input=output, stylesheet=self.assets('antizebra.xsl'))
        # then binary marc
        output = shellout("yaz-marcdump -i marcxml -o marc {input} > {output}",
                          input=output)
        # then marcuniq
        output = shellout("marcuniq -o {output} {input}", input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class RISMJson(RISMTask):
    """ Convert MARC to JSON. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return RISMMarc(date=self.date)

    @timed
    def run(self):
        output = shellout("marctojson -m date={date} {input} > {output}",
                          input=self.input().path, date=self.closest())
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class RISMIndex(RISMTask, CopyToIndex):
    date = ClosestDateParameter(default=datetime.date.today())

    index = 'rism'
    doc_type = 'title'

    mapping = {'title': {'date_detection': False,
                          '_id': {'path': 'content.001'},
                          '_all': {'enabled': True,
                                   'term_vector': 'with_positions_offsets',
                                   'store': True}}}

    def update_id(self):
        """ This id will be a unique identifier for this indexing task."""
        return self.effective_task_id()

    def requires(self):
        return RISMJson(date=self.date)
