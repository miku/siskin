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

from siskin.benchmark import timed
from gluish.common import Executable
from gluish.format import TSV
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.utils import memoize
from siskin.task import DefaultTask
import BeautifulSoup
import datetime
import luigi
import re

class WikidataTask(DefaultTask):
    """
    Base task for wikidata.
    """
    TAG = 'wikidata'

    @memoize
    def closest(self):
        if not hasattr(self, 'date'):
            raise RuntimeError('Task has no date: %s' % self)
        task = WikidataLatestDate(date=self.date)
        luigi.build([task], local_scheduler=True)
        with task.output().open() as handle:
            date = handle.iter_tsv(cols=('date', 'url')).next().date
        result = datetime.date(*(int(v) for v in date.split('-')))
        return result

class WikidataLatestDate(WikidataTask):
    """
    For a given date, return the closest date in the past on which wikidata shipped.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return WikidataDumpDates()

    @timed
    def run(self):
        """ load dates and urls in urlmap, use a custom key
        function to find the closest date, but double check,
        if that date actually lies in the future - if it does, raise
        an exception, otherwise dump the closest date and filename to output """
        urlmap = {}
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('date', 'url')):
                date = datetime.datetime.strptime(row.date, '%Y-%m-%d').date()
                urlmap[date] = row.url

        def closest_keyfun(date):
            if date > self.date:
                return datetime.timedelta(999999999)
            return abs(date - self.date)

        closest = min(urlmap.keys(), key=closest_keyfun)
        if closest > self.date:
            raise RuntimeError('No shipment before: %s' % min(urlmap.keys()))

        with self.output().open('w') as output:
            output.write_tsv(closest, urlmap.get(closest))

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class WikidataDumpDates(WikidataTask):
    """ Dump dates and links like this:

    2014-02-10  http://dumps.wikimedia.org/wikidatawiki/20140210/
    2014-02-26  http://dumps.wikimedia.org/wikidatawiki/20140226/
    2014-03-15  http://dumps.wikimedia.org/wikidatawiki/20140315/
    ...
    """
    date = luigi.DateParameter(default=datetime.date.today())

    @timed
    def run(self):
        output = shellout("""wget --retry-connrefused http://dumps.wikimedia.org/wikidatawiki/ -O {output}""")
        with open(output) as handle:
            soup = BeautifulSoup.BeautifulSoup(handle.read())

        pattern = re.compile(r".*?(\d{8,8}).*?")
        dates = []
        for link in soup.findAll('a'):
            if pattern.match(link.text):
                date = datetime.date(int(link.text[0:4]), int(link.text[4:6]),
                                     int(link.text[6:8]))
                url = "http://dumps.wikimedia.org/wikidatawiki/%s" % link['href']
                dates.append((date, url))

        with self.output().open('w') as output:
            for date, url in sorted(dates):
                output.write_tsv(date, url)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class WikidataDumpLinks(WikidataTask):
    """ A wikidata dump will consist of many files. Dump all links for those files. """

    date = luigi.DateParameter(default=datetime.date.today())

    @timed
    def run(self):
        url = "http://dumps.wikimedia.org/wikidatawiki/%s/" % (self.closest().strftime('%Y%m%d'))
        output = shellout("""wget --retry-connrefused {url} -O {output}""", url=url)

        with open(output) as handle:
            soup = BeautifulSoup.BeautifulSoup(handle.read())

        pattern = re.compile("wikidatawiki-.*")
        links = set()
        for link in soup.findAll('a'):
            if pattern.match(link.text):
                date = self.closest().strftime('%Y%m%d')
                url = "http://dumps.wikimedia.org/wikidatawiki/%s/%s" % (date, link.text)
                links.add(url)

        with self.output().open('w') as output:
            for url in sorted(links):
                output.write_tsv(url)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class WikidataFile(WikidataTask):
    """
    Download a single file with a certain suffix out of the dump files.
    The most important is pages-articles.xml.bz2, which is the default.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    suffix = luigi.Parameter(default='pages-articles.xml.bz2')

    timeout = luigi.IntParameter(default=10, significant=False)

    def requires(self):
        return WikidataDumpLinks(date=self.date)

    @timed
    def run(self):
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('url',)):
                if row.url.endswith(self.suffix):
                    output = shellout("""curl --connect-timeout {timeout} {url} | bunzip2 -c > {output}""",
                                      url=row.url, timeout=self.timeout)
                    luigi.File(output).move(self.output().path)
                    break
            else:
                raise RuntimeError('No URL with suffix: %s, try to rerun WikidataDumpLinks' % self.suffix)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True, ext='xml'))

class WikiPagesJson(WikidataTask):
    """
    Generic conversion from XML to JSON.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'dump': WikidataFile(suffix='pages-articles.xml.bz2', date=self.date),
                'app': Executable(name='wikidatatojson', message='https://github.com/miku/wikitools')}

    @timed
    def run(self):
        output = shellout("wikidatatojson {input} > {output}", input=self.input().get('dump').path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True, ext='ldj'))
