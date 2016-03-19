# coding: utf-8

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

from gluish.intervals import yearly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
import datetime
import json
import luigi

class BartocTask(DefaultTask):
    """ Bartoc. """
    TAG = 'bartoc'

    def closest(self):
        return yearly(self.date)

class BartocDump(BartocTask):
    """ Dump JSON. http://bartoc.org/en/node/761 """

    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("""wget -O {output} 'http://bartoc.org/en/download/json?download=1&eid=3224' """)
        with open(output) as handle:
            try:
                _ = json.load(handle)
            except ValueError as err:
                raise RuntimeError('invalid download: %s' % err)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='json'))
