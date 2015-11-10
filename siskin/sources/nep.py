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

"""
NEP

Configuration keys:

[core]

google-username = user.name
google-password = secret
google-docs-key = crypticstring

[nep]

# location of archived shipments
backlog-host = example.com
backlog-username = username
backlog-pattern = /path/to/data/*.upd.zip

# location of recent shipments
ftp-host = host.name
ftp-username = username
ftp-password = password
ftp-pattern = some*glob*pattern.zip

# dates to ignore
mute = 2014-01-01, 2014-05-07

"""

from gluish.format import TSV
from gluish.intervals import hourly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from luigi.contrib.esindex import CopyToIndex
from siskin.benchmark import timed
from siskin.common import Directory, FTPMirror
from siskin.configuration import Config
from siskin.task import DefaultTask
from siskin.utils import copyregions, memoize
import datetime
import glob
import gspread
import json
import luigi
import marcx
import os
import pymarc
import re
import string
import tempfile

config = Config.instance()

class NEPTask(DefaultTask):
    TAG = '003'

    def muted(self):
        """
        Return a set of muted dates.
        """
        mute_value = config.get('nep', 'mute', '')
        try:
            vals = mute_value.split(',')
            return set([datetime.date(*map(int, s.split('-'))) for s in vals])
        except ValueError:
            raise RuntimeError('failed to parse mutes: {0}'.format(mute_value))

    @memoize
    def closest(self):
        if not hasattr(self, 'date'):
            raise RuntimeError('Task has no date: %s' % self)
        task = NEPLatestDate(date=self.date)
        luigi.build([task], local_scheduler=True)
        with task.output().open() as handle:
            date = handle.iter_tsv(cols=('date', 'path')).next().date
        return datetime.date(*(int(v) for v in date.split('-')))

class NEPCopy(NEPTask):

    indicator = luigi.Parameter(default=hourly(fmt='%s'))

    server = luigi.Parameter(default=config.get('nep', 'backlog-host'))
    username = luigi.Parameter(default=config.get('nep', 'backlog-username'))
    pattern = luigi.Parameter(default=config.get('nep', 'backlog-pattern'))

    timeout = luigi.IntParameter(default=15, description='rsnyc IO timeout', significant=False)

    def requires(self):
        return Directory(path=os.path.dirname(self.output().path))

    @timed
    def run(self):
        shellout("""rsync --timeout {timeout} -avz {username}@{server}:{pattern} {directory}""",
                 timeout=self.timeout, username=self.username, server=self.server,
                 pattern=self.pattern, directory=self.input().path)

        with self.output().open('w') as output:
            pattern = os.path.join(self.input().path,
                                   os.path.basename(self.pattern))
            for path in glob.glob(pattern):
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True), format=TSV)

class NEPImport(NEPTask):
    """ NEPCopy and FTP. """

    indicator = luigi.Parameter(default=hourly(fmt='%s'))

    def requires(self):
        return {'copy': NEPCopy(),
                'sync': FTPMirror(host=config.get('nep', 'ftp-host'),
                                  username=config.get('nep', 'ftp-username'),
                                  password=config.get('nep', 'ftp-password'),
                                  pattern=config.get('nep', 'ftp-pattern'))}

    @timed
    def run(self):
        target = os.path.dirname(self.output().path)
        for key in ('copy', 'sync'):
            with self.input().get(key).open() as handle:
                for row in handle.iter_tsv(cols=('path',)):
                    basename = os.path.basename(row.path)
                    destination = os.path.join(target, basename)
                    if not os.path.exists(destination):
                        dstdir = os.path.dirname(destination)
                        if not os.path.exists(dstdir):
                            os.makedirs(dstdir)
                        if not os.path.exists(destination):
                            shellout("ln -s {src} {dst}", src=row.path, dst=destination)

        p = os.path.basename(config.get('nep', 'ftp-pattern'))
        pattern = os.path.join(target, p)
        with self.output().open('w') as output:
            for path in sorted(glob.glob(pattern)):
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class NEPDatesAndPaths(NEPTask):
    indicator = luigi.Parameter(default=hourly(fmt='%s'))

    def requires(self):
        return NEPImport()

    @timed
    def run(self):
        """ Parse `nep.ignore`. Extract all dates from the filenames. """
        entries = set()
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                basename = os.path.basename(row.path)
                match = re.search(r'.*_([0-9]{8})_.*', basename)
                if match:
                    date_obj = datetime.datetime.strptime(match.group(1), '%Y%m%d').date()
                    if not date_obj in self.muted():
                        entries.add((date_obj, row.path))
                    else:
                        self.logger.debug('ignoring %s via nep.mute' % date_obj)

        with self.output().open('w') as output:
            for date, path in sorted(entries):
                output.write_tsv(date, path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class NEPLatestDate(NEPTask):
    """ For a given date, return the closest date in the past
    on which EBL shipped. """
    date = luigi.DateParameter(default=datetime.date.today())
    indicator = luigi.Parameter(default=hourly(fmt='%s'))

    def requires(self):
        return NEPDatesAndPaths()

    @timed
    def run(self):
        """ load dates and paths in pathmap, use a custom key
        function to find the closest date, but double check,
        if that date actually lies in the future - if it does, raise
        an exception, otherwise dump the closest date and filename to output """
        pathmap = {}
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('date', 'path')):
                date = datetime.date(*(int(v) for v in row.date.split('-')))
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

class NEPDates(NEPTask):
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return NEPDatesAndPaths()

    @timed
    def run(self):
        output = shellout("LANG=C awk '{{print $1}}' {input} | LANG=C sort -u > {output}",
                        input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class NEPLatestDateAndPaths(NEPTask):
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return NEPDatesAndPaths()

    @timed
    def run(self):
        with self.input().open() as handle:
            with self.output().open('w') as output:
                for row in handle.iter_tsv(cols=('date', 'path')):
                    date = datetime.date(*(int(v) for v in row.date.split('-')))
                    if date == self.closest():
                        output.write_tsv(*row)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class NEPCombine(NEPTask):
    """ Combine into a single file. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return NEPLatestDateAndPaths(date=self.date)

    @timed
    def run(self):
        _, combined = tempfile.mkstemp(prefix='tasktree-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('date', 'path')):
                shellout("unzip -p {input} >> {output}", input=row.path,
                         output=combined)
        luigi.File(combined).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class NEPTable(NEPTask):
    """ (ID STATUS DATE OFFSET LENGTH) for a single NEP shipments.
    Assumes that a single shipment of NEP will contain no duplicate record IDs. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return NEPCombine(date=self.date)

    @timed
    def run(self):
        mm = shellout("marcmap {input} | LANG=C sort -k1,1 > {output}", input=self.input().path)
        st = shellout("marctotsv {input} 001 @Status {date} | LANG=C sort -k1,1 > {output}", input=self.input().path, date=self.closest())
        output = shellout("paste {status} {marcmap} | cut -f 1,2,3,5,6 > {output}", status=st, marcmap=mm)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class NEPEvents(NEPTask):
    """ (ID STATUS DATE OFFSET LENGTH) for all NEP shipments. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        task = NEPDates()
        luigi.build([task], local_scheduler=True)
        with task.output().open() as handle:
            for row in handle.iter_tsv(cols=('date',)):
                date = datetime.date(*(int(v) for v in row.date.split('-')))
                if date < self.closest():
                    yield NEPTable(date=date)

    @timed
    def run(self):
        _, combined = tempfile.mkstemp(prefix='tasktree-')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path,
                     output=combined)
        output = shellout("LANG=C sort -k1,1 -k3,3 {input} > {output}", input=combined)
        luigi.File(output).move(self.output().fn)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class NEPSurface(NEPTask):
    """ Only keep the most recent versions of a record (row). """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return NEPEvents(date=self.date)

    @timed
    def run(self):
        surface = {}
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('id', 'status', 'X', 'X', 'X')):
                if row.status in ('a', 'c', 'n', 'p'):
                    # older entries get overwritten
                    surface[row.id] = row
                elif row.status in ('d',):
                    del surface[row.id]

        _, stopover = tempfile.mkstemp(prefix='tasktree-')
        with luigi.File(path=stopover, format=TSV).open('w') as output:
            for _, row in surface.iteritems():
                output.write_tsv(*row)

        output = shellout("LANG=C sort -k3,3 -k4,4n {input} > {output}",
                          input=stopover)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class NEPSnapshot(NEPTask):
    """ Create the snapshot by utilizing seekmaps/surface. This will create
    a single MARC binary. As of Fall 2013, this file is about 1G in size.
    Non-pandas version.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return NEPSurface(date=self.date)

    @timed
    def run(self):
        output = shellout("cut -f3 {input} | LANG=C sort | LANG=C uniq > {output}", input=self.input().path)
        with open(output) as handle:
            dates = map(string.strip, handle.readlines())

        with self.output().open('w') as output:
            for date in dates:
                task = NEPCombine(date=datetime.date(*(int(v) for v in date.split('-'))))
                luigi.build([task], local_scheduler=True)
                with task.output().open() as fh:
                    seekfile = shellout("""LANG=C grep "{date}" "{input}" | cut -f 4,5 > {output}""", date=str(date), input=self.input().path)
                    with luigi.File(seekfile, format=TSV).open() as handle:
                        seekmap = ((int(offset), int(length)) for offset, length in handle.iter_tsv())
                        copyregions(fh, output, seekmap)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class NEPSubjectFilterCodes(NEPTask):
    """
    Take a file from assets and use as
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        luigi.File(self.assets("nep_subject_filter_codes.tsv")).copy(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class NEPForUBL(NEPTask):
    """
    Filter NEP by bicssc codes. And other filters, relevant for UBL.

        marctotsv -f NA -s '|' .../nep-snapshot/2013-11-20.mrc \
            007 240.a 072.2 072.a |
            grep ^t | awk -F'\t' '$2 ~ "NA"' | awk -F'\t' '$3 ~ "bicssc"'

    would get you almost there. TODO: Learn more awk.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'codes': NEPSubjectFilterCodes(date=self.date),
                'snaphost': NEPSnapshot(date=self.date)}

    @timed
    def run(self):
        with self.input().get('codes').open() as handle:
            filter_codes = map(string.strip, handle.readlines())

        with self.input().get('snaphost').open() as handle:
            with self.output().open('w') as output:
                reader = pymarc.MARCReader(handle, to_unicode=True)
                writer = pymarc.MARCWriter(output)
                for record in reader:
                    record = marcx.FatRecord.from_record(record)

                    # 240.a Uniform title
                    if record.has('240.a'):
                        continue

                    # 007 Physical Description Fixed Field, Text
                    if not record.test('007', lambda s: s.startswith('t')):
                        continue

                    # 072.2 Subject Category Code, Source
                    if record.test('072.2', lambda s: s.startswith('bicssc')):
                        for code in record.itervalues('072.a'):
                            if code in filter_codes:
                                writer.write(record.to_record())
                                break

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class NEPJson(NEPTask):
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return NEPForUBL(date=self.date)

    @timed
    def run(self):
        output = shellout("marctojson -m date={date} {input} > {output}",
                          input=self.input().path, date=self.closest())
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='json'))

class NEPIndex(NEPTask, CopyToIndex):
    date = ClosestDateParameter(default=datetime.date.today())

    index = 'nep'
    doc_type = 'title'
    purge_existing_index = True

    settings = {
        "settings": {
            "number_of_shards": 5,
            "analysis": {
                "filter": {
                    "autocomplete_filter": {
                        # normal ngram vs edge?
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
                            # add more?
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
                                # multifield?
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
        return NEPJson(date=self.date)
