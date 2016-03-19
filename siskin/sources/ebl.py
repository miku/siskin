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
EBL.

Configuration keys:

[ebl]

ftp-host = host.name
ftp-username = username
ftp-password = password
ftp-path = /
ftp-pattern = some*glob*pattern.zip

# Dates to ignore.
exclude = 1970-12-31

# This regexes are matched against the file paths. It must provide `year`, `month` and `day` named groups.
regex-dump = .*(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})-(\d{4})_leip_FULL_CATALOGUE_export.zip
regex-delta = .*(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})-(\d{4})_leip_Content_export.zip

glob-add = *leip_Content_Add_*.mrc
glob-delete = *leip_Content_Delete_*.mrc
"""

from gluish.format import TSV
from gluish.intervals import hourly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from luigi.contrib.esindex import CopyToIndex
from siskin.benchmark import timed
from siskin.common import FTPMirror, Executable
from siskin.configuration import Config
from siskin.database import sqlitedb
from siskin.task import DefaultTask
from siskin.utils import memoize
import base64
import datetime
import luigi
import operator
import re
import tempfile

config = Config.instance()

class EBLTask(DefaultTask):
    TAG = '004'

    @memoize
    def muted(self):
        """ Parse ebl.config and return a set of dates to ignore. """
        try:
            dates = config.get('ebl', 'exclude', '').split(',')
            return set([datetime.date(*(int(v) for v in s.split('-'))) for s in dates])
        except ValueError as err:
            raise RuntimeError('could not parse ebl.exclude: %s' % err)

    @memoize
    def closest(self):
        """ Just use the output of `EBLLatestDate` here.
        Any exception occuring in `EBLLatestDate` will propagate,
        e.g. if a too early date is requested. """
        if not hasattr(self, 'date'):
            raise RuntimeError('cannot find closest date for a task w/o date')
        task = EBLLatestDate(date=self.date)
        luigi.build([task], local_scheduler=True)
        s = task.output().open().read().strip()
        result = datetime.date(*(int(v) for v in s.split('-')))
        return result

class EBLPaths(EBLTask):
    """ A list of EBL file paths (via FTP). """
    indicator = luigi.Parameter(default=hourly(fmt='%s'))

    def requires(self):
        host = config.get('ebl', 'ftp-host')
        username = config.get('ebl', 'ftp-username')
        password = config.get('ebl', 'ftp-password')
        base = config.get('ebl', 'ftp-path')
        pattern = config.get('ebl', 'ftp-pattern')
        return FTPMirror(host=host, username=username, password=password,
                         base=base, pattern=pattern)

    @timed
    def run(self):
        self.input().move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class EBLInventory(EBLTask):
    """ List EBL inventory in form of (type, date, path) tuples. No sorting. """
    indicator = luigi.Parameter(default=hourly(fmt='%s'))

    def requires(self):
        return EBLPaths()

    def run(self):
        getdate = operator.itemgetter('year', 'month', 'day')
        patterns = (
            ('dump', re.compile(config.get('ebl', 'regex-dump'))),
            ('delta', re.compile(config.get('ebl', 'regex-delta'))),
        )
        with self.input().open() as handle:
            with self.output().open('w') as output:
                for row in handle.iter_tsv(cols=('path',)):
                    for name, pattern in patterns:
                        match = pattern.search(row.path)
                        if match:
                            date = datetime.date(*map(int, getdate(match.groupdict())))
                            if date in self.muted():
                                continue
                            output.write_tsv(name, date, row.path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class EBLBacklog(EBLTask):
    """ A list of all task relevant for a certain date. Includes dumps and deltas.
    Will raise an error, if there no dump can be found. Ordered by date, dump first. """

    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return EBLInventory()

    def run(self):
        backlog = []
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('kind', 'date', 'path')):
                date = datetime.date(*map(int, row.date.split('-')))
                if date <= self.date:
                    backlog.append(row)
        if all([entry.kind == 'delta' for entry in backlog]):
            raise RuntimeError('No EBL dump found before %s' % self.date)
        backlog = sorted(backlog, key=operator.itemgetter(1), reverse=True)
        index = [entry.kind for entry in backlog].index('dump') + 1
        backlog = reversed(backlog[:index])
        with self.output().open('w') as output:
            for entry in backlog:
                output.write_tsv(*entry)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class EBLDumpCombined(EBLTask):
    """ Combine .mrc files out of dump. """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return EBLInventory()

    def run(self):
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('kind', 'date', 'path')):
                date = datetime.date(*map(int, row.date.split('-')))
                if date == self.date and row.kind == 'dump':
                    output = shellout("unzip -p {input} \*.mrc > {output}", input=row.path,
                                      ignoremap={1: 'A warning alone will trigger a non-zero return value. Assuming everything went well anyway.'})
                    luigi.File(output).move(self.output().path)
                    break
            else:
                raise RuntimeError("No EBL dump on given date: %s" % self.date)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class EBLDeltaCombined(EBLTask):
    """ Out of a delta, extract and combine the additions or deletions. """

    date = luigi.DateParameter(default=datetime.date.today())
    kind = luigi.Parameter(default='add', description='add or delete')

    def requires(self):
        return EBLInventory()

    def run(self):
        patterns = {
            'add': config.get('ebl', 'glob-add'),
            'delete': config.get('ebl', 'glob-delete'),
        }
        if self.kind not in patterns:
            raise RuntimeError("Unknown kind, only valid kinds are add and delete.")
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('kind', 'date', 'path')):
                date = datetime.date(*map(int, row.date.split('-')))
                if date == self.date and row.kind == 'delta':
                    output = shellout("unzip -p {input} \{pattern} > {output}", input=row.path, pattern=patterns[self.kind],
                                      ignoremap={1: 'A warning alone will trigger a non-zero return value. Assuming everything went well anyway.'})
                    luigi.File(output).move(self.output().path)
                    break
            else:
                raise RuntimeError("No EBL delta package on given date: %s" % self.date)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class EBLMarcDB(EBLTask):
    """ Create a sqlite3 db with (id, secondary (date), blob) table to allow random access
    to a single marc records by id for a certain date. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return EBLBacklog(date=self.closest())

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('kind', 'date', 'path')):
                date = datetime.date(*map(int, row.date.split('-')))
                if row.kind == 'delta':
                    task = EBLDeltaCombined(date=date, kind='add')
                    luigi.build([task])
                    shellout("marcdb -encode -secondary {date} -o {output} {input}", date=date, input=task.output().path, output=stopover)
                if row.kind == 'dump':
                    task = EBLDumpCombined(date=date)
                    luigi.build([task])
                    shellout("marcdb -encode -secondary {date} -o {output} {input}", date=date, input=task.output().path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='db'))

class EBLEvents(EBLTask):
    """ Given a date, create a single MARC file, that incorporates the dump and all deltas (additions, deletions). """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return EBLBacklog(date=self.closest())

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('kind', 'date', 'path')):
                date = datetime.date(*map(int, row.date.split('-')))
                if row.kind == 'dump':
                    task = EBLDumpCombined(date=date)
                    luigi.build([task])
                    shellout("marctotsv {input} U 001 {date} >> {output}", input=task.output().path, date=date, output=stopover)
                if row.kind == 'delta':
                    task = EBLDeltaCombined(date=date, kind='add')
                    luigi.build([task])
                    shellout("marctotsv {input} U 001 {date} >> {output}", input=task.output().path, date=date, output=stopover)
                    task = EBLDeltaCombined(date=date, kind='delete')
                    luigi.build([task])
                    shellout("marctotsv {input} D 001 {date} >> {output}", input=task.output().path, date=date, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class EBLSurface(EBLTask):
    """ For a given date, list the (EBL ID, date) that should be in the current
    state of the data source. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return EBLEvents(date=self.date)

    @timed
    def run(self):
        datemap = {}
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('kind', 'id', 'date')):
                date = datetime.date(*map(int, row.date.split('-')))
                if row.kind == 'U':
                    datemap[row.id] = date
                if row.kind == 'D':
                    if row.id in datemap:
                        del datemap[row.id]
                    else:
                        # just another indication of a deleted record that
                        # was not added before - ignore
                        pass

        with self.output().open('w') as output:
            for id, date in sorted(datemap.iteritems(), key=operator.itemgetter(1)):
                output.write_tsv(id, date)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class EBLSnapshot(EBLTask):
    """ Create a single MARC file, that represents the state of the data for
    a given date. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'surface': EBLSurface(date=self.date),
                'db': EBLMarcDB(date=self.date)}

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with sqlitedb(self.input().get('db').path) as cursor:
            with self.input().get('surface').open() as handle:
                with open(stopover, 'wb') as output:
                    for row in handle.iter_tsv(cols=('id', 'date')):
                        cursor.execute("SELECT record from store where id = ? and secondary = ?", (row.id, row.date))
                        result = cursor.fetchone()
                        output.write(base64.b64decode(result[0]))
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class EBLLatestDateAndPath(EBLTask):
    """ For a given date, return the closest date in the past
    on which EBL shipped (dump or delta). """
    date = luigi.DateParameter(default=datetime.date.today())
    indicator = luigi.Parameter(default=hourly(fmt='%s'))

    def requires(self):
        return EBLInventory(indicator=self.indicator)

    @timed
    def run(self):
        """ load dates and paths in pathmap, use a custom key
        function to find the closest date, but double check,
        if that date actually lies in the future - if it does, raise
        an exception, otherwise dump the closest date and filename to output """
        pathmap = {}
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('kind', 'date', 'path')):
                date = datetime.datetime.strptime(row.date, '%Y-%m-%d').date()
                pathmap[date] = row.path

        def closest_keyfun(date):
            if date > self.date:
                return datetime.timedelta(1e8)
            return abs(date - self.date)

        closest = min(pathmap.keys(), key=closest_keyfun)
        if closest > self.date:
            raise RuntimeError('No shipment before: %s' % min(pathmap.keys()))

        with self.output().open('w') as output:
            output.write_tsv(closest, pathmap.get(closest))

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class EBLLatestDate(EBLTask):
    """ Only output the latest date. """
    date = luigi.DateParameter(default=datetime.date.today())
    indicator = luigi.Parameter(default=hourly(fmt='%s'))

    def requires(self):
        return EBLLatestDateAndPath(date=self.date, indicator=self.indicator)

    @timed
    def run(self):
        output = shellout("awk '{{print $1}}' {input} > {output}",
                          input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class EBLJson(EBLTask):
    """ Take EBL combined and convert it to JSON. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'marc': EBLSnapshot(date=self.date),
                'converter': Executable(name='marctojson', message='http://git.io/1LXpQA')}

    @timed
    def run(self):
        output = shellout("marctojson -m date={date} {input} > {output}",
                          input=self.input().get('marc').fn,
                          date=self.closest())
        luigi.File(output).move(self.output().fn)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='json'))

class EBLIndex(EBLTask, CopyToIndex):
    """ Index EBL. """
    date = ClosestDateParameter(default=datetime.date.today())

    index = 'ebl'
    doc_type = 'title'
    purge_existing_index = True

    settings = {
        "settings": {
            "number_of_shards": 5,
            "number_of_replicas": 0,
            "analysis": {
                "filter": {
                    "autocomplete_filter": {
                        "type":     "edge_ngram",
                        "min_gram": 1,
                        "max_gram": 20
                    }
                },
                "analyzer": {
                    "autocomplete": {
                        "type":      "custom",
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "autocomplete_filter"
                        ]
                    }
                }
            }
        }
    }

    mapping = {
        'title': {
            'date_detection': False,
            '_id': {
                'path': 'content.001'
            },
            '_all': {
                'enabled': True,
                'term_vector': 'with_positions_offsets',
                'store': True
            },
            'properties': {
                'content': {
                    'properties': {
                        '245': {
                            'properties': {
                                'a': {
                                    'type': 'string',
                                    'index_analyzer': 'autocomplete',
                                    'search_analyzer': 'standard'
                                }
                            }
                        },
                        '100': {
                            'properties': {
                                'a': {
                                    'type': 'string',
                                    'index_analyzer': 'autocomplete',
                                    'search_analyzer': 'standard'
                                }
                            }
                        },
                        '700': {
                            'properties': {
                                'a': {
                                    'type': 'string',
                                    'index_analyzer': 'autocomplete',
                                    'search_analyzer': 'standard'
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    def update_id(self):
        """ This id will be a unique identifier for this indexing task."""
        return self.effective_task_id()

    def requires(self):
        return EBLJson(date=self.date)
