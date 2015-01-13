#!/usr/bin/env python

"""
CrossRef is an association of scholarly publishers that develops shared
infrastructure to support more effective scholarly communications.

Our citation-linking network today covers over 68 million journal articles
and other content items (books chapters, data, theses, technical reports)
from thousands of scholarly and professional publishers around the globe.
"""

from gluish.common import ElasticsearchMixin
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import date_range, shellout
from siskin.task import DefaultTask
from siskin.utils import URLCache
import datetime
import json
import luigi
import os
import requests
import tempfile
import urllib

class CrossrefTask(DefaultTask):
    """
    Crossref source, http://www.crossref.org/
    """
    TAG = 'crossref'

    def closest(self):
        """ Do monthly updates. """
        return monthly(date=self.date)

class CrossrefHarvestChunk(CrossrefTask):
    """
    API docs can be found under: http://api.crossref.org/

    The output file is line delimited JSON, just the concatenated responses.
    """
    begin = luigi.DateParameter()
    end = luigi.DateParameter()
    filter = luigi.Parameter(default='index', description='index, deposit, update')
    rows = luigi.IntParameter(default=1000, significant=False)

    max_retries = luigi.IntParameter(default=10, significant=False)

    def run(self):
        cache = URLCache(directory=os.path.join(tempfile.gettempdir(), '.urlcache'))
        adapter = requests.adapters.HTTPAdapter(max_retries=self.max_retries)
        cache.sess.mount('http://', adapter)

        filter = "from-{self.filter}-date:{self.begin},until-{self.filter}-date:{self.end}".format(self=self)
        rows, offset = self.rows, 0

        with self.output().open('w') as output:
            while True:
                params = {"rows": rows, "offset": offset, "filter": filter}
                url = 'http://api.crossref.org/works?%s' % (urllib.urlencode(params))
                body = cache.get(url)
                try:
                    content = json.loads(body)
                except ValueError as err:
                    self.logger.debug(err)
                    self.logger.debug(body)
                    raise
                items = content["message"]["items"]
                self.logger.debug("%s: %s" % (url, len(items)))
                if len(items) == 0:
                    break
                output.write(body)
                output.write("\n")
                offset += rows

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class CrossrefHarvest(luigi.WrapperTask, CrossrefTask):
    """
    Harvest everything in incremental steps.
    """
    begin = luigi.DateParameter(default=datetime.date(1970, 1, 1))
    end = luigi.DateParameter()
    filter = luigi.Parameter(default='index', description='index, deposit, update')
    rows = luigi.IntParameter(default=1000, significant=False)

    def requires(self):
        dates = date_range(self.begin, self.end, 1, 'months')
        for i, _ in enumerate(dates[:-1]):
            yield CrossrefHarvestChunk(begin=dates[i], end=dates[i + 1], rows=self.rows, filter=self.filter)

    def output(self):
        return self.input()

class CrossrefCombine(CrossrefTask):
    """
    Combine all harvested files into a single LDJ file.
    Might contain dups, since `from-index-date` and `until-index-date` are
    both inclusive.
    """
    begin = luigi.DateParameter(default=datetime.date(1970, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())
    filter = luigi.Parameter(default='index', description='index, deposit, update')
    rows = luigi.IntParameter(default=1000, significant=False)

    def requires(self):
        return CrossrefHarvest(begin=self.begin, end=self.closest(), rows=self.rows, filter=self.filter)

    def run(self):
        _, combined = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path, output=combined)
        luigi.File(combined).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class CrossrefItems(CrossrefTask):
    """
    Combine all harvested files into a single LDJ file.
    Flatten and deduplicate. Stub.
    """
    begin = luigi.DateParameter(default=datetime.date(1970, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())
    filter = luigi.Parameter(default='index', description='index, deposit, update')
    rows = luigi.IntParameter(default=1000, significant=False)

    def requires(self):
        return CrossrefHarvest(begin=self.begin, end=self.closest(), rows=self.rows, filter=self.filter)

    def run(self):
        with self.output().open('w') as output:
            for target in self.input():
                with target.open() as handle:
                    for line in handle:
                        content = json.loads(line)
                        if not content.get("status") == "ok":
                            raise RuntimeError("invalid response status")
                        items = content["message"]["items"]
                        for item in items:
                            output.write(json.dumps(item))
                            output.write("\n")

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class CrossrefIndex(CrossrefTask, ElasticsearchMixin):
    """ Vanilla records. """

    begin = luigi.DateParameter(default=datetime.date(1970, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())
    index = luigi.Parameter(default='crossref')

    def requires(self):
        return CrossrefItems(begin=self.begin, date=self.date)

    def run(self):
        shellout("curl -XDELETE {host}:{port}/{index}", host=self.es_host, port=self.es_port, index=self.index)
        shellout("esbulk -index {index} {input}", index=self.index, input=self.input().path)
        with self.output().open('w'):
            pass

    def output(self):
        return luigi.LocalTarget(path=self.path())
