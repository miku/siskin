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

from gluish.common import Executable
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from siskin.task import DefaultTask
from gluish.utils import shellout
import datetime
import luigi


class TUFGoobiTask(DefaultTask):
    TAG = 'tufgoobi'

    def closest(self):
        return monthly(date=self.date)

class TUFGoobiDump(TUFGoobiTask):
    """
    Complete dump.
    """
    def requires(self):
        return Executable(name='oaimi', message='https://github.com/miku/oaimi')

    def run(self):
        output = shellout("""
            cat <(echo '<collection xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">')
                <(oaimi -verbose http://digital.ub.tu-freiberg.de/oai)
                <(echo '</collection>') > {output}""")
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))


# class TUFGoobiCombine(TUFGoobiTask):
#     """ Digital library via Goobi (#3207) """
#     begin = luigi.DateParameter(default=datetime.date(2000, 1, 1))
#     date = ClosestDateParameter(default=datetime.date.today())
#     prefix = luigi.Parameter(default="oai_dc", significant=False)
#     url = luigi.Parameter(default="http://digital.ub.tu-freiberg.de/oai", significant=False)
#     collection = luigi.Parameter(default=None, significant=False)

#     def requires(self):
#         return OAIHarvestChunk(begin=self.begin, end=self.date,
#                                prefix=self.prefix, url=self.url,
#                                collection=self.collection)

#     def run(self):
#         output = shellout("xsltproc {stylesheet} {input} > {output}",
#                           input=self.input().path,
#                           stylesheet=self.assets('OAIDCtoMARCXML.xsl'))
#         output = shellout("yaz-marcdump -i marcxml -o marc {input} > {output}",
#                           input=output)
#         luigi.File(output).move(self.output().path)

#     def output(self):
#         return luigi.LocalTarget(path=self.path(ext='mrc'))
