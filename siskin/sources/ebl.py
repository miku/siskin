# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

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

# This regex is matched against the file paths. It must provide `year`, `month` and `day` named groups.
path-regex = .*(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})-.*.zip
"""

from gluish.benchmark import timed
from gluish.common import FTPMirror, Executable
from gluish.esindex import CopyToIndex
from gluish.format import TSV
from gluish.intervals import hourly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout, memoize, random_string
from siskin.configuration import Config
from siskin.task import DefaultTask
import datetime
import json
import luigi
import operator
import re

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
            ('dump', re.compile(".*(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})-(\d{4})_leip_FULL_CATALOGUE_export.zip")),
            ('delta', re.compile(".*(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})-(\d{4})_leip_Content_export.zip")),
        )
        with self.input().open() as handle:
            with self.output().open('w') as output:
                for row in handle.iter_tsv(cols=('path',)):
                    for name, pattern in patterns:
                        match = pattern.search(row.path)
                        if match:
                            date = datetime.date(*map(int, getdate(match.groupdict())))
                            output.write_tsv(name, date, row.path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class EBLDatesAndPaths(EBLTask):
    """ Dump the dates and file paths to a file sorted. """
    indicator = luigi.Parameter(default=random_string())

    def requires(self):
        return EBLPaths(indicator=self.indicator)

    @timed
    def run(self):
        pattern = re.compile(config.get('ebl', 'path-regex'))
        with self.input().open() as handle:
            with self.output().open('w') as output:
                for row in sorted(handle.iter_tsv(cols=('path',))):
                    mo = pattern.match(row.path)
                    if not mo:
                        raise RuntimeError('unknown EBL pattern: %s, %s' % (row.path, pattern))
                    gdict = mo.groupdict()
                    date = datetime.date(int(gdict['year']), int(gdict['month']),
                                         int(gdict['day']))
                    if date in self.muted():
                        self.logger.debug("Skipping %s since it is muted." % date)
                    else:
                        output.write_tsv(date, row.path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class EBLDates(EBLTask):
    """ All EBL dates sorted in a single file. """
    indicator = luigi.Parameter(default=hourly(fmt='%s'))

    def requires(self):
        return EBLDatesAndPaths(indicator=self.indicator)

    def run(self):
        output = shellout("awk '{{print $1}}' {input} > {output}",
                          input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class EBLLatestDateAndPath(EBLTask):
    """ For a given date, return the closest date in the past
    on which EBL shipped. """
    date = luigi.DateParameter(default=datetime.date.today())
    indicator = luigi.Parameter(default=hourly(fmt='%s'))

    def requires(self):
        return EBLDatesAndPaths(indicator=self.indicator)

    @timed
    def run(self):
        """ load dates and paths in pathmap, use a custom key
        function to find the closest date, but double check,
        if that date actually lies in the future - if it does, raise
        an exception, otherwise dump the closest date and filename to output """
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

class EBLCombine(EBLTask):
    """ Unzip the file contents MARC files into a single file. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return EBLLatestDateAndPath(date=self.date)

    @timed
    def run(self):
        with self.input().open() as handle:
            path = handle.iter_tsv(cols=('date', 'path')).next().path
        output = shellout("unzip -p {input} \*.mrc > {output}", input=path,
                          ignoremap={1: 'A warning alone will trigger a non-zero return value. Assuming everything went well anyway.'})
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class EBLJson(EBLTask):
    """ Take EBL combined and convert it to JSON. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'marc': EBLCombine(date=self.date),
                'converter': Executable(name='marctojson'),}

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
