#!/usr/bin/env python
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
The EZB.

Configuration keys:

[ezb]

# url to uncompressed TSV file
url = http://example.org/file/61f1456.tab
"""

from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.configuration import Config
from siskin.task import DefaultTask
from siskin.utils import ElasticsearchMixin
import csv
import datetime
import json
import luigi

config = Config.instance()

class EZBTask(DefaultTask):
    """ EZB related tasks. """
    TAG = 'ezb'

    def closest(self):
        return datetime.date(2015, 1, 1)

class EZBTSVImport(EZBTask):
    """ Download TSV from URL. """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("""wget --retry-connrefused -O {output} "{url}" """, url=config.get('ezb', 'url'))
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

class EZBJson(EZBTask):
    """ Convert TSV to LDJ. """
    date = ClosestDateParameter(default=datetime.date.today())
    skiphead = luigi.IntParameter(default=5, significant=False)
    encoding = luigi.Parameter(default='iso-8859-1', significant=False)

    def requires(self):
        return EZBTSVImport(date=self.date)

    def run(self):
        cols = ['ezb-id', 'title', 'color', 'publisher', 'subject', 'eissn', 'pissn',
                'zdb-id', 'frontdoor-url', 'type', 'price-type', 'access', 'link',
                'anchor', 'first-year', 'first-volume', 'first-issue', 'last-year',
                'last-volume', 'last-issue', 'moving-wall', 'available']
        with self.input().open() as handle:
            with self.output().open('w') as output:
                for i in range(self.skiphead):
                    handle.next()
                reader = csv.reader(handle, delimiter='\t')
                for row in reader:
                    r = [c.decode(self.encoding) for c in row]
                    output.write(json.dumps(dict(zip(cols, r))))
                    output.write("\n")

    def output(self):
        return luigi.LocalTarget(path=self.path())

class EZBIndex(EZBTask, ElasticsearchMixin):
    """ Index EZB data. """
    date = ClosestDateParameter(default=datetime.date.today())
    skiphead = luigi.IntParameter(default=5, significant=False)
    encoding = luigi.Parameter(default='iso-8859-1', significant=False)
    index = luigi.Parameter(default='ezb')

    def requires(self):
        return EZBJson(date=self.date, skiphead=self.skiphead, encoding=self.encoding)

    def run(self):
        shellout("curl -XDELETE {host}:{port}/{index}", host=self.es_host, port=self.es_port, index=self.index)
        shellout("esbulk -verbose -index {index} {input}", index=self.index, input=self.input().path)
        with self.output().open('w'):
            pass

    def output(self):
        return luigi.LocalTarget(path=self.path())
