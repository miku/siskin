#!/usr/bin/env python

"""
Crossref stub.
"""

from siskin.task import DefaultTask
import luigi
import requests
import urllib

class CrossrefTask(DefaultTask):
    TAG = 'crossref'

class CrossrefHarvestChunk(CrossrefTask):
    """
    http://api.crossref.org/works?rows=50&offset=10&filter=from-pub-date:1900-01-01,until-pub-date:1900-01-02
    """
    begin = luigi.DateParameter()
    end = luigi.DateParameter()

    def run(self):
        rows, offset = 10, 0
        with self.output().open('w') as output:
            while True:
                params = {"rows": rows,
                          "offset": offset,
                          "from-pub-date": str(self.begin),
                          "until-pub-date": str(self.end)}
                url = "http://api.crossref.org/works?%s" % urllib.urlencode(params)
                r = requests.get(url)
                if r.status_code == 200:
                    content = json.loads(r.text)
                    if len(content["message"]["items"]) == 0:
                        break
                    output.write(content)
                    output.write("\n")
                    offset += 50
                else:
                    raise RuntimeError("%s on %s" % (r.status_code, url))

    def output(self):
        return luigi.LocalTarget(path=self.path())
