# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

from gluish.benchmark import timed
from gluish.common import Executable
from gluish.format import TSV
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout, memoize
from siskin.task import DefaultTask
import BeautifulSoup
import datetime
import luigi
import re

class WikidataTask(DefaultTask):
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
    """ For a given date, return the closest date in the past
    on which EBL shipped. """
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

    date = luigi.DateParameter(default=datetime.date.today())

    @timed
    def run(self):
        output = shellout("""wget --retry-connrefused
                             http://dumps.wikimedia.org/wikidatawiki/
                             -O {output}""")
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
    """ Dump all links for a dump date. """

    date = luigi.DateParameter(default=datetime.date.today())

    @timed
    def run(self):
        url = "http://dumps.wikimedia.org/wikidatawiki/%s/" % (self.closest().strftime('%Y%m%d'))
        output = shellout("""wget --retry-connrefused
                             {url} -O {output}""", url=url)

        with open(output) as handle:
            soup = BeautifulSoup.BeautifulSoup(handle.read())

        pattern = re.compile("wikidatawiki-.*")
        links = set()
        for link in soup.findAll('a'):
            if pattern.match(link.text):
                url = "http://dumps.wikimedia.org/wikidatawiki/%s/%s" % (
                    (self.closest().strftime('%Y%m%d'), link.text))
                links.add(url)

        with self.output().open('w') as output:
            for url in sorted(links):
                output.write_tsv(url)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class WikidataFile(WikidataTask):

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
                    output = None
                    if row.url.endswith('.gz'):
                        output = shellout("""curl --connect-timeout {timeout} {url} | gunzip -c > {output}""",
                                          url=row.url, timeout=self.timeout)
                    elif row.url.endswith('.bz2'):
                        output = shellout("""curl --connect-timeout {timeout} {url} | bunzip2 -c > {output}""",
                                          url=row.url, timeout=self.timeout)
                    else:
                        output = shellout("""wget --retry-connrefused -q {url}
                                             -O {output}""", url=row.url)
                    if output is None:
                        raise RuntimeError('no output')
                    luigi.File(output).move(self.output().path)
                    break
            else:
                raise RuntimeError('No URL matches parameters. '
                                   'Try to rerun WikidataDumpLinks task')

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True, ext='xml'))

class WikiPagesJson(WikidataTask):

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'dump': WikidataFile(suffix='pages-articles.xml.bz2', date=self.date),
                'app': Executable(name='wikidatatojson', message='https://github.com/miku/wikitools'))}

    @timed
    def run(self):
        output = shellout("wikidatatojson {input} > {output}", input=self.input().get('dump').path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True, ext='ldj'))
