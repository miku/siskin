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
import datetime
import json
import luigi
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
    rows = luigi.IntParameter(default=1000, significant=False)
    max_retries = luigi.IntParameter(default=10, significant=False)

    def run(self):
        sess = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=self.max_retries)
        sess.mount('http://', adapter)

        filter = "from-index-date:%s,until-index-date:%s" % (str(self.begin), str(self.end))
        rows, offset = self.rows, 0

        with self.output().open('w') as output:
            while True:
                params = {"rows": rows, "offset": offset, "filter": filter}
                url = "http://api.crossref.org/works?%s" % urllib.urlencode(params)
                r = sess.get(url)
                if r.status_code == 200:
                    content = json.loads(r.text)
                    items = content["message"]["items"]
                    self.logger.debug("%s: %s" % (url, len(items)))
                    if len(items) == 0:
                        break
                    output.write(r.text)
                    output.write("\n")
                    offset += rows
                else:
                    raise RuntimeError("%s on %s" % (r.status_code, url))

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class CrossrefHarvest(luigi.WrapperTask, CrossrefTask):
    """
    Harvest everything in incremental steps.
    """
    begin = luigi.DateParameter(default=datetime.date(1970, 1, 1))
    end = luigi.DateParameter()
    rows = luigi.IntParameter(default=1000, significant=False)

    def requires(self):
        dates = date_range(self.begin, self.end, 1, 'months')
        for i, _ in enumerate(dates[:-1]):
            yield CrossrefHarvestChunk(begin=dates[i], end=dates[i + 1], rows=self.rows)

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
    rows = luigi.IntParameter(default=1000, significant=False)

    def requires(self):
        return CrossrefHarvest(begin=self.begin, end=self.closest(), rows=self.rows)

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
    rows = luigi.IntParameter(default=1000, significant=False)

    def requires(self):
        return CrossrefHarvest(begin=self.begin, end=self.closest(), rows=self.rows)

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
