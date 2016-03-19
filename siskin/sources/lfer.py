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

"""
LFER.

Configuration keys:

[lfer]

# glob that catches all LFER files
glob = /path/to/lfer/SA-MARCcomb-lfer-*.tar.gz

# regular expression to get the date out of the path
pattern = ".*SA-MARCcomb-lfer-(?P<year>\d\d)(?P<month>\d\d)(?P<day>\d\d).tar.gz"

"""

from gluish.format import TSV
from gluish.intervals import hourly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from luigi.contrib.esindex import CopyToIndex
from siskin.benchmark import timed
from siskin.common import Directory
from siskin.configuration import Config
from siskin.task import DefaultTask
from siskin.utils import iterfiles
import datetime
import json
import luigi
import os
import re

config = Config.instance()

class LFERTask(DefaultTask):
    TAG = '008'

    KIND_FILENAME_MAP = {
        'tit': 'lfer-tit.mrc',
        'lok': 'lfer-lok.mrc',
        'aut': 'lfer-aut.mrc',
    }

    def closest(self):
        if not hasattr(self, 'date'):
            raise RuntimeError('task has not date attribute: %s' % self)
        task = LFERLatestDate(date=self.date)
        luigi.build([task], local_scheduler=True)
        s = task.output().open().read().strip()
        result = datetime.date(*(int(v) for v in s.split('-')))
        return result

class LFERSync(LFERTask):
    """ Copy all LFER files over. """
    indicator = luigi.Parameter(default=hourly(fmt='%s'))

    def requires(self):
        return Directory(path=os.path.dirname(self.output().path))

    @timed
    def run(self):
        target = os.path.dirname(self.output().path)
        pattern = os.path.join(config.get('lfer', 'glob'))
        shellout("rsync -avz {src} {target}", src=pattern, target=target)
        with self.output().open('w') as output:
            for path in sorted(iterfiles(target)):
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)

class LFERDatesAndPaths(LFERTask):
    """ Just emit a two column TSV with (date, path). """
    indicator = luigi.Parameter(default=hourly(fmt='%s'))

    def requires(self):
        return LFERSync(indicator=self.indicator)

    @timed
    def run(self):
        pattern = re.compile(config.get('lfer', 'pattern'))
        with self.input().open() as handle:
            with self.output().open('w') as output:
                for row in handle.iter_tsv(cols=('path',)):
                    match = pattern.match(row.path)
                    if match:
                        gd = match.groupdict()
                        year, month, day = map(int, ('20%s' % gd['year'], gd['month'], gd['day']))
                        date = datetime.date(year, month, day)
                        output.write_tsv(date, row.path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class LFERLatestDateAndPath(LFERTask):
    indicator = luigi.Parameter(default=hourly(fmt='%s'))
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return LFERDatesAndPaths(indicator=self.indicator)

    @timed
    def run(self):
        pathmap = {}
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('date', 'path')):
                date = datetime.datetime.strptime(row.date, '%Y-%m-%d').date()
                pathmap[date] = row.path

        def closest_keyfun(date):
            if date > self.date:
                return datetime.timedelta(999999999)
            return abs(date - self.date)

        closest = min(pathmap.keys(), key=closest_keyfun)
        if closest > self.date:
            raise RuntimeError('No shipment before: %s' % min(pathmap.keys()))

        with self.output().open('w') as output:
            output.write_tsv(closest, pathmap.get(closest))

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class LFERLatestDate(LFERTask):
    """ Only output the latest date. """
    date = luigi.DateParameter(default=datetime.date.today())
    indicator = luigi.Parameter(default=hourly(fmt='%s'))

    def requires(self):
        return LFERLatestDateAndPath(date=self.date, indicator=self.indicator)

    @timed
    def run(self):
        output = shellout("awk '{{print $1}}' {input} > {output}",
                          input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class LFERMarc(LFERTask):
    """ Import a single date or fail if there was no shipment on that date. """
    date = ClosestDateParameter(default=datetime.date.today())
    kind = luigi.Parameter(default='tit')

    def requires(self):
        return LFERLatestDateAndPath(date=self.date)

    @timed
    def run(self):
        if self.kind not in self.KIND_FILENAME_MAP:
            raise ValueError('allowed: tit, lok, aut, but got: %s' % self.kind)

        # the filename inside the archive
        filename = self.KIND_FILENAME_MAP.get(self.kind)

        # the latest shipment relative to self.date
        with self.input().open() as handle:
            date, path = handle.iter_tsv(cols=('date', 'path')).next()
        self.logger.debug("The closest shipment occured at {0}".format(date))

        # stream file out
        output = shellout("tar -zOxf {archive} {fn} > {output}",
                          archive=path, fn=filename)
        # move
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class LFERJson(LFERTask):
    date = ClosestDateParameter(default=datetime.date.today())
    kind = luigi.Parameter(default='tit')

    def requires(self):
        return LFERMarc(date=self.date, kind=self.kind)

    @timed
    def run(self):
        tmp = shellout("marctojson -m date={date} {input} > {output}",
                       input=self.input().fn, date=self.closest())
        luigi.File(tmp).move(self.output().fn)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='json'))

class LFERIndex(LFERTask, CopyToIndex):
    date = ClosestDateParameter(default=datetime.date.today())

    index = 'lfer'
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
        return LFERJson(date=self.date, kind='tit')
