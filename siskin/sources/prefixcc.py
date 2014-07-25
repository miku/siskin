# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301,C0103,W0614,W0401

from gluish.format import TSV
from gluish.intervals import yearly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
import BeautifulSoup
import datetime
import HTMLParser
import luigi
import operator


class PrefixCCTask(DefaultTask):
    TAG = 'prefixcc'

    def closest(self):
        return yearly(date=self.date)

class PrefixCCDownload(PrefixCCTask):
    """ Download common ns prefixes, about 1k. """
    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://prefix.cc/popular/all.rdf", significant=False)

    def run(self):
        output = shellout("wget --retry-connrefused '{url}' -O {output}", url=self.url)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='html'))

class PrefixCCParse(PrefixCCTask):
    """ Parse out. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return PrefixCCDownload()

    def run(self):
        with self.input().open() as handle:
            soup = BeautifulSoup.BeautifulSoup(handle.read())
        lines = map(lambda s: HTMLParser.HTMLParser().unescape(s.strip()),
                    soup.find("pre").text.split("\n"))

        shortest = {}        
        for line in sorted(lines):
            if not line.startswith('xmlns:'):
                continue
            parts = line.split("=")
            if not len(parts) == 2:
                continue
            head, rest = parts
            head = head.replace('xmlns:', '')
            rest = rest.strip('"')
            if rest in shortest:
                if len(head) < len(shortest[rest]):
                    shortest[rest] = head
            else:
                shortest[rest] = head
        
        with self.output().open('w') as output:
            for rest, head in sorted(shortest.iteritems(), key=operator.itemgetter(1)):
                output.write_tsv(head, rest)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
