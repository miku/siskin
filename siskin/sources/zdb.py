# coding: utf-8
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

from siskin.benchmark import timed
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
import datetime
import luigi
import urllib

class ZDBTask(DefaultTask):
    TAG = 'zdb'

    def closest(self):
        return monthly(date=self.date)

class ZDBDump(ZDBTask):
    """ Download the ZDB snapshot. """

    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='ttl', description='alternatively: rdf')

    @timed
    def run(self):
        host = "datendienst.dnb.de"
        path = "/cgi-bin/mabit.pl"
        params = {
            "userID": "opendata",
            "pass": "opendata",
            "cmd": "fetch",
            "mabheft": "ZDBTitel.%s.gz" % self.format,
        }

        url = 'http://{host}{path}?{params}'.format(host=host,
                                                    path=path,
                                                    params=urllib.urlencode(params))
        output = shellout("""wget --retry-connrefused "{url}" -O {output}""", url=url)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='%s.gz' % self.format))

class ZDBExtract(ZDBTask):
    """ Extract the archive. """

    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='ttl', description='alternatively: rdf')

    def requires(self):
        return ZDBDump(date=self.date, format=self.format)

    @timed
    def run(self):
        output = shellout("gunzip -c {input} > {output}", input=self.input().fn)
        luigi.File(output).move(self.output().fn)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext=self.format))

class ZDBNTriples(ZDBTask):
    """ Get a Ntriples representation of ZDB. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return ZDBExtract(date=self.date)

    @timed
    def run(self):
        output = shellout("serdi -b -i turtle -o ntriples {input} > {output}",
                          input=self.input().path)
        # output = shellout("sort {input} > {output}", input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='nt'))
