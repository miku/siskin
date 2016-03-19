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
OSO.

This group of task worked once, but OSO completely redesigned their site,
so there tasks will fail now.
"""

from gluish.format import TSV
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from luigi.contrib.esindex import CopyToIndex
from siskin.benchmark import timed
from siskin.database import sqlitedb
from siskin.task import DefaultTask
from siskin.utils import copyregions, iterfiles
import BeautifulSoup
import datetime
import luigi
import os
import tempfile
import urlparse

class OSOTask(DefaultTask):
    """ Oxford Scholarship Online. """
    TAG = '018'

    def closest(self):
        return monthly(self.date)

class OSODownloadPage(OSOTask):
    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://www.universitypressscholarship.com/page/1250/oso-marc-archive",
                          significant=False)

    @timed
    def run(self):
        output = shellout("wget -q --retry-connrefused -O {output} '{url}' ", url=self.url)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='html'))

class OSOMarcFiles(OSOTask):
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return OSODownloadPage(date=self.date)

    @timed
    def run(self):
        with self.input().open() as handle:
            soup = BeautifulSoup.BeautifulSoup(handle.read())
        with self.output().open('w') as output:
            for anchor in soup.findAll('a'):
                href = anchor.get('href')
                if not href.endswith('mrc'):
                    continue
                output.write_tsv(href)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class OSOMarcDownload(OSOTask):
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return OSOMarcFiles(date=self.date)

    def run(self):
        base = "http://www.universitypressscholarship.com/"

        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                dirname, basename = row.path.split('/')[-2:]
                slugged = dirname.replace('%20', '-').lower()
                url = urlparse.urljoin(base, row.path)
                dst = os.path.join(self.taskdir(), '{0}-{1}'.format(slugged, basename))
                if os.path.exists(dst):
                    continue
                output = shellout("""wget --retry-connrefused "{url}" -O {output} """, url=url)
                luigi.File(output).move(dst)

        with self.output().open('w') as output:
            for path in iterfiles(self.taskdir()):
                if not path.endswith('mrc'):
                    continue
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)

class OSOCombine(OSOTask):
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return OSOMarcDownload(date=self.date)

    def run(self):
        _, combined = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                shellout('cat {input} >> {output}', input=row.path, output=combined)
        luigi.File(combined).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'), format=TSV)

class OSOSeekmap(OSOTask):
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return OSOCombine(date=self.date)

    def run(self):
        output = shellout('marcmap -o {output} {input}', input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='db'))

class OSOSurface(OSOTask):
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return OSOCombine(date=self.date)

    def run(self):
        output = shellout("marctotsv {input} 001 005 > {output}",
                          input=self.input().path)
        output = shellout("LANG=C sort {input} > {output}", input=output)
        output = shellout("LANG=C tac {input} | LANG=C uniq -w 13 > {output}", input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class OSOSnapshot(OSOTask):
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'surface': OSOSurface(date=self.date),
                'seekmap': OSOSeekmap(date=self.date),
                'file': OSOCombine(date=self.date)}

    def run(self):
        with self.input().get('surface').open() as handle:
            with self.output().open('w') as output:
                with self.input().get('file').open() as fh:
                    with sqlitedb(self.input().get('seekmap').path) as cursor:
                        regions = []
                        for row in handle.iter_tsv(cols=('id', 'date')):
                            cursor.execute("SELECT offset, length FROM seekmap where id = ?", (row.id,))
                            regions.append(cursor.fetchone())
                        copyregions(fh, output, regions)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class OSOJson(OSOTask):
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return OSOSnapshot(date=self.date)

    @timed
    def run(self):
        output = shellout("marctojson -m date={date} {input} > {output}",
                          date=self.date, input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class OSOIndex(OSOTask, CopyToIndex):
    date = ClosestDateParameter(default=datetime.date.today())

    index = 'oso'
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
        return OSOJson(date=self.date)
