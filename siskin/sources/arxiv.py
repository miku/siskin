# coding: utf-8

"""
arxiv.org

http://arxiv.org/help/oa/index

----

From: https://groups.google.com/forum/#!topic/arxiv-api/aOacIt6KD2E

> the problem is that the API is not the appropriate vehicle to obtain a full
  copy of arXiv metadata. It is intended for interactive queries and search overlays
  and should not be used for full replication. The size of result sets is capped at a large but finite number.
  You should use OAI-PMH instead for your purposes.

"""

from gluish.benchmark import timed
from gluish.common import OAIHarvestChunk
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout, date_range
from siskin.task import DefaultTask
import datetime
import luigi
import tempfile

class ArxivTask(DefaultTask):
    """ Base task. """

    def closest(self):
        return monthly(date=self.date)

class ArxivHarvestChunk(OAIHarvestChunk, ArxivTask):
    """ Harvest a chunk. """

    begin = luigi.DateParameter(default=datetime.date.today())
    end = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://export.arxiv.org/oai2", significant=False)
    prefix = luigi.Parameter(default="oai_dc", significant=False)
    delay = luigi.IntParameter(default=60, significant=False)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))

class ArxivHarvest(luigi.WrapperTask, ArxivTask):
    """ Harvest arxiv. """
    begin = luigi.DateParameter(default=datetime.date(2007, 1, 1))
    end = luigi.DateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://export.arxiv.org/oai2", significant=False)
    prefix = luigi.Parameter(default="oai_dc", significant=False)

    def requires(self):
        """ Only require up to the last full month. """
        begin = datetime.date(self.begin.year, self.begin.month, 1)
        end = datetime.date(self.end.year, self.end.month, 1)
        if end < self.begin:
            raise RuntimeError('Invalid range: %s - %s' % (begin, end))
        dates = date_range(begin, end, 7, 'days')
        for i, _ in enumerate(dates[:-1]):
            yield ArxivHarvestChunk(begin=dates[i], end=dates[i + 1], url=self.url, prefix=self.prefix)

    def output(self):
        return self.input()

class ArxivCombine(ArxivTask):
    """ Combine the chunks for a date range into a single file.
    The filename will carry a name, that will only include year
    and month for begin and end. """
    begin = luigi.DateParameter(default=datetime.date(2011, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://export.arxiv.org/oai2", significant=False)
    prefix = luigi.Parameter(default="oai_dc", significant=False)

    def requires(self):
        """ monthly updates """
        return ArxivHarvest(begin=self.begin, end=self.date, prefix=self.prefix, url=self.url)

    @timed
    def run(self):
        xsl = self.assets("OAIDCtoMARCXML.xsl")
        _, combined = tempfile.mkstemp(prefix='siskin-')
        for target in self.input().get('files'):
            output = shellout("xsltproc {stylesheet} {input} > {output}",
                              stylesheet=xsl, input=target.path)
            output = shellout("yaz-marcdump -i marcxml -o marc {input} > {output}",
                              input=output)
            shellout("cat {input} >> {output}", input=output, output=combined)

        output = shellout("marcuniq -o {output} {input}", input=combined)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))
