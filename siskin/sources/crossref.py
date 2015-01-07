#!/usr/bin/env python

"""
CrossRef is an association of scholarly publishers that develops shared
infrastructure to support more effective scholarly communications.

Our citation-linking network today covers over 68 million journal articles
and other content items (books chapters, data, theses, technical reports)
from thousands of scholarly and professional publishers around the globe.
"""

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
    rows = luigi.IntParameter(default=100, significant=False)

    def run(self):
        if self.rows < 1:
            raise RuntimeError('rows parameter must be positive')
        filter = "from-index-date:%s,until-index-date:%s" % (str(self.begin), str(self.end))
        rows, offset = self.rows, 0
        with self.output().open('w') as output:
            while True:
                params = {"rows": rows, "offset": offset, "filter": filter}
                url = "http://api.crossref.org/works?%s" % urllib.urlencode(params)
                r = requests.get(url)
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
    rows = luigi.IntParameter(default=100, significant=False)

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
    rows = luigi.IntParameter(default=100, significant=False)

    def requires(self):
        return CrossrefHarvest(begin=self.begin, end=self.closest(), rows=self.rows)

    def run(self):
        _, combined = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path, output=combined)
        luigi.File(combined).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))
