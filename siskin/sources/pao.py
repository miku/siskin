# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101

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

"""
PA Online

Configuration keys:

[pao]

scp-src = [user@server:[port]]/path/to/file/*mrc
deletions = [user@server:[port]]/path/to/paodel.mrc

"""

from gluish.common import Executable
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from luigi.contrib.esindex import CopyToIndex
from siskin.benchmark import timed
from siskin.configuration import Config
from siskin.task import DefaultTask
from siskin.utils import iterfiles
import datetime
import luigi
import re
import tempfile

config = Config.instance()

class PAOTask(DefaultTask):
    TAG = '007'

    def closest(self):
        """ One time data source. """
        return datetime.date(2014, 1, 1)

class PAOSync(PAOTask):
    date = ClosestDateParameter(default=datetime.date.today())

    @timed
    def run(self):
        stopover = tempfile.mkdtemp(prefix='siskin-')
        shellout("scp {origin} {stopover}".format(origin=config.get('pao', 'scp-src'), stopover=stopover))
        _, combined = tempfile.mkstemp(prefix='siskin-')
        for path in iterfiles(directory=stopover,
                              fun=lambda path: re.search(r'pao[\d].mrc', path)):
            shellout("cat {path} >> {output}", path=path, output=combined)
        luigi.File(combined).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class PAODeletions(PAOTask):
    date = ClosestDateParameter(default=datetime.date.today())

    @timed
    def run(self):
        output = shellout("scp {deletions} {output}", deletions=config.get('pao', 'deletions'))
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class PAOJson(PAOTask):
    """ Convert to JSON, respect deletions. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'additions': PAOSync(date=self.date),
                'deletions': PAODeletions(date=self.date),
                'apps': [Executable(name='marctotsv'),
                         Executable(name='marcuniq')]
        }

    @timed
    def run(self):
        # sanitize
        additions = shellout("""yaz-marcdump -f iso-8859-1 -t utf-8
                             -i marc -o marc {input} > {output}""",
                             input=self.input().get('additions').path)
        deletions = shellout("""yaz-marcdump -f iso-8859-1 -t utf-8
                             -i marc -o marc {input} > {output}""",
                             input=self.input().get('deletions').path)

        # filter deletions
        excludes = shellout("""marctotsv {input} 001 > {output} """,
                            input=deletions)
        output = shellout("""marcuniq -x {excludes} -o {output} {input} """,
                       excludes=excludes, input=additions)

        # to json
        output = shellout("marctojson -m date={date} {input} > {output}",
                          date=self.closest(), input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class PAOIndex(PAOTask, CopyToIndex):
    date = ClosestDateParameter(default=datetime.date.today())

    index = 'pao'
    doc_type = 'title'
    purge_existing_index = True

    mapping = {'title': {'date_detection': False,
                          '_id': {'path': 'content.001'},
                          '_all': {'enabled': True,
                                   'term_vector': 'with_positions_offsets',
                                   'store': True}}}

    def update_id(self):
        """ This id will be a unique identifier for this indexing task."""
        return self.effective_task_id()

    def requires(self):
        return PAOJson(date=self.date)
