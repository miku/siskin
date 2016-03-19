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
SWB Open Data.
"""

from gluish.format import TSV
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from luigi.contrib.esindex import CopyToIndex
from siskin.benchmark import timed
from siskin.database import sqlitedb
from siskin.task import DefaultTask
from siskin.utils import copyregions
import BeautifulSoup
import collections
import datetime
import luigi
import operator
import re
import requests
import string
import tempfile
import urlparse

class SWBOpenDataTask(DefaultTask):
    TAG = '025'

    def closest(self):
        if not hasattr(self, 'date'):
            raise RuntimeError('no date attribute on %s' % self)
        task = SWBOpenDataLatestDate(date=self.date)
        luigi.build([task], local_scheduler=True)
        with task.output().open() as handle:
            s = handle.read().strip()
            date = datetime.date(*map(int, s.split('-')))
        return date

class SWBOpenDataLatestDate(SWBOpenDataTask):
    """ For a given date, return the closest date in the past
    on which EBL shipped. """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return SWBOpenDataInventory()

    @timed
    def run(self):
        """ load dates and paths in pathmap, use a custom key
        function to find the closest date, but double check,
        if that date actually lies in the future - if it does, raise
        an exception, otherwise dump the closest date and filename to output """
        dates = set()
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('date', 'type', 'url')):
                date = datetime.date(*map(int, row.date.split('-')))
                dates.add(date)

        def closest_keyfun(date):
            if date > self.date:
                return datetime.timedelta(999999999)
            return abs(date - self.date)

        closest = min(dates, key=closest_keyfun)
        if closest > self.date:
            raise RuntimeError('No shipment before: %s' % min(dates))

        with self.output().open('w') as output:
            output.write_tsv(closest)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class SWBOpenDataInventory(SWBOpenDataTask):
    base = luigi.Parameter(default="http://swblod.bsz-bw.de/od/", significant=False)
    date = luigi.DateParameter(default=datetime.date.today())

    @timed
    def run(self):
        r = requests.get(self.base)
        if not r.status_code == 200:
            raise RuntimeError("HTTP %s: %s" % (r.status_code, r.text))
        soup = BeautifulSoup.BeautifulSoup(r.text)
        links = soup.findAll('a')
        packages = collections.defaultdict(set)
        patterns = {'update': r'od-up_bsz-tit_([\d]{6})_([\d]{1,}).xml.tar.gz',
                    'dump': r'od_bsz-tit_([\d]{6})_([\d]{1,}).xml.tar.gz'}
        for link in links:
            filename = link.get('href')
            kind, pattern = None, None
            if filename.startswith('od-up_bsz'):
                pattern = re.compile(patterns.get('update'))
                kind = 'update'
            elif filename.startswith('od_bsz'):
                pattern = re.compile(patterns.get('dump'))
                kind = 'dump'
            else:
                continue

            match = pattern.match(filename)
            if not match:
                raise RuntimeError("unexpected filename: %s" % filename)

            date = datetime.datetime.strptime(match.group(1), '%y%m%d').date()
            packages[date].add((kind, filename))

        with self.output().open('w') as output:
            for date in sorted(packages):
                for kind, filename in sorted(packages[date]):
                    url = urlparse.urljoin(self.base, filename)
                    output.write_tsv(date, kind, url)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class SWBOpenDataMarc(SWBOpenDataTask):
    """ Get the MARC version of SWB OD for some date. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return SWBOpenDataInventory()

    @timed
    def run(self):
        """ Assumes every tarball contains a single file. Will fail during
        XML -> MARC conversion otherwise. """
        urls = set()
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('date', 'kind', 'url')):
                dateobj = datetime.date(*map(int, row.date.split('-')))
                if dateobj == self.closest():
                    urls.add(row.url)

        _, combined = tempfile.mkstemp(prefix='tasktree-')
        for url in urls:
            output = shellout("wget --retry-connrefused -O {output} '{url}'", url=url)
            output = shellout("tar -Oxzf {input} > {output}", input=output)
            shellout("yaz-marcdump -i marcxml -o marc {input} >> {output}", input=output, output=combined)
        luigi.File(combined).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class SWBOpenDataSeekMapDB(SWBOpenDataTask):
    date = ClosestDateParameter(default=datetime.date.today())
    def requires(self):
        return SWBOpenDataMarc(date=self.date)

    @timed
    def run(self):
        output = shellout("marcmap -o {output} {input}", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='db'))

class SWBOpenDataListified(SWBOpenDataTask):
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return SWBOpenDataMarc(date=self.date)

    @timed
    def run(self):
        output = shellout("""
            marctotsv -f NA -s "|" {input} 001 {date} 005 924.b > {output}""",
            date=self.closest(), input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class SWBOpenDataListifiedRange(SWBOpenDataTask):
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        task = SWBOpenDataDates(date=self.date)
        luigi.build([task], local_scheduler=True)
        with task.output().open() as handle:
            for row in handle.iter_tsv(cols=('date',)):
                dateobj = datetime.date(*map(int, row.date.split('-')))
                yield SWBOpenDataListified(date=dateobj)

    @timed
    def run(self):
        _, combined = tempfile.mkstemp(prefix='tasktree')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path, output=combined)
        output = shellout("LANG=C sort -k1,1 -k2,2 {input} > {output}", input=combined)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

class SWBOpenDataDates(SWBOpenDataTask):
    """ Just all the dates up to the last dump. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return SWBOpenDataInventory()

    @timed
    def run(self):
        dates = set()
        with self.input().open() as handle:
            sequence = list(handle.iter_tsv(cols=('date', 'type', 'url')))
            for row in reversed(sequence):
                dateobj = datetime.date(*map(int, row.date.split('-')))
                if dateobj > self.date:
                    continue
                dates.add(row.date)
                if row.type == 'dump':
                    break
        if not dates:
            raise RuntimeError('no shipments before %s' % self.date)

        with self.output().open('w') as output:
            for date in sorted(dates):
                output.write_tsv(date)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class SWBOpenDataSnapshot(SWBOpenDataTask):
    """ Find the latest entries from SWB OD. Non-pandas version. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return SWBOpenDataListifiedRange(date=self.date)

    @timed
    def run(self):
        output = shellout("""tac "{input}" | LANG=C uniq -w 9 | LANG=C cut -f 1,2 | LANG=C sort > {output}""", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class SWBOpenDataSnapshotMarcX(SWBOpenDataTask):
    """ Try marcsnapshot for snapshots. High memory overhead for now. """
    date = ClosestDateParameter(default=datetime.date.today())
    limit = luigi.IntParameter(default=50000, significant=False, description='limit for seekmapdb queries')

    def requires(self):
        prerequisite = SWBOpenDataDates(date=self.date)
        luigi.build([prerequisite])
        with prerequisite.output().open() as handle:
            for row in handle.iter_tsv(cols=('date',)):
                yield SWBOpenDataMarc(date=row.date)

    def run(self):
        files = [target.path for target in self.input()]
        output = shellout("""marcsnapshot -verbose -o {output} {files}""", files=' '.join(files))
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class SWBOpenDataSnapshotMarc(SWBOpenDataTask):
    """ Create a single MARC file, that represents
    the current state of affairs. """
    date = ClosestDateParameter(default=datetime.date.today())
    limit = luigi.IntParameter(default=50000, significant=False, description='limit for seekmapdb queries')

    def requires(self):
        return SWBOpenDataSnapshot(date=self.date)

    @timed
    def run(self):
        output = shellout("cut -f2 {input} | LANG=C sort | LANG=C uniq > {output}", input=self.input().path)
        with open(output) as handle:
            dates = map(string.strip, handle.readlines())

        with self.output().open('w') as output:
            for date in dates:
                dateobj = datetime.date(*map(int, date.split('-')))
                marc = SWBOpenDataMarc(date=dateobj)
                sdb = SWBOpenDataSeekMapDB(date=dateobj)
                luigi.build([marc, sdb], local_scheduler=True)
                with open(marc.output().path) as handle:
                    with sqlitedb(sdb.output().path) as cursor:
                        idset = df[df.date == date].id.values.tolist()
                        limit, offset = self.limit, 0
                        while True:
                            cursor.execute("""
                                SELECT offset, length
                                FROM seekmap WHERE id IN (%s)""" % (
                                    ','.join(("'%s'" % id for id in idset[offset:offset + limit]))))
                            rows = cursor.fetchall()
                            if not rows:
                                break
                            else:
                                copyregions(handle, output, rows)
                                offset += limit

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class SWBOpenDataSnapshotJson(SWBOpenDataTask):
    """ Create a single JSON file, that represents
    the current state of affairs. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return SWBOpenDataSnapshotMarc(date=self.date)

    @timed
    def run(self):
        output = shellout("marctojson -m date={date} {input} > {output}", input=self.input().path, date=self.closest())
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

class SWBOpenDataIndex(SWBOpenDataTask, CopyToIndex):
    date = ClosestDateParameter(default=datetime.date.today())

    index = 'swbod'
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
        return SWBOpenDataSnapshotJson(date=self.date)

class SWBOpenDataRVK(SWBOpenDataTask):
    """ Find the RVK categories per PPN. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return SWBOpenDataSnapshotMarc(date=self.date)

    def run(self):
        output = shellout("""marctotsv -s "|" {input} 001 936.a > {output}""", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class SWBOpenDataRVKModel(SWBOpenDataTask):
    """ Find the coocurrences of RVK categories. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return SWBOpenDataRVK(date=self.date)

    def run(self):
        cooc = collections.defaultdict(list)
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('ppn', 'rvks')):
                rvks = row.rvks.split('|')
                for key in rvks:
                    for rvk in rvks:
                        if not rvk == key:
                            cooc[key].append(rvk)

        with self.output().open('w') as output:
            for k, v in cooc.iteritems():
                output.write_tsv(k, '|'.join(v))

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class SWBOpenDataRVKTopCooccurences(SWBOpenDataTask):
    """ Report the top N coocurences only. Optionally
    only take into account cooccurences set sizes greater or equal `size`.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    top = luigi.IntParameter(default=5)
    size = luigi.IntParameter(default=5)

    def requires(self):
        return SWBOpenDataRVKModel(date=self.date)

    def run(self):
        with self.input().open() as handle:
            with self.output().open('w') as output:
                for row in handle.iter_tsv(cols=('rvk', 'rvks')):
                    rvks = row.rvks.split('|')
                    total = len(rvks)
                    if total == 0:
                        continue
                    counter = collections.defaultdict(int)
                    for rvk in rvks:
                        counter[rvk] += 1
                    if len(counter.keys()) < self.size:
                        continue
                    topn = sorted([(k, v) for k, v in counter.iteritems()], key=operator.itemgetter(1), reverse=True)[:self.top]
                    topn = map(operator.itemgetter(0), topn)
                    output.write_tsv(row.rvk, *topn)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
