# coding: utf-8

from gluish.common import Executable
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.sources.crossref import CrossrefDOIList
from siskin.task import DefaultTask
import datetime
import luigi

class DOITask(DefaultTask):
    """
    A task for doi.org related things.
    """
    TAG = 'doi'

    def closest(self):
        return monthly(self.date)

class DOICheck(DOITask):
    """
    Harvest DOI redirects from doi.org API.

    ---- FYI ----

    It is highly recommended that you put a static entry into /etc/hosts
    for doi.org while `hurrly` is running.

    As of 2015-07-31 doi.org resolves to six servers. Just choose one.

        $ nslookup doi.org

    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'input': CrossrefDOIList(date=self.date),
                'hurrly': Executable(name='hurrly', message='http://github.com/miku/hurrly')}

    def run(self):
        output = shellout("hurrly -w 64 < {input} | gzip > {output}", input=self.input().get('input').path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='tsv.gz'))

class DOIBlacklist(DOITask):
    """
    Create a blacklist of DOIs. Possible cases:

    1. A DOI redirects to http://www.crossref.org/deleted_DOI.html
    2. A DOI API lookup results in a HTTP 4XX.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return DOICheck(date=self.date)

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout("""LC_ALL=C zgrep -F "http://www.crossref.org/deleted_DOI.html" {input} >> {output}""",
                 input=self.input().path, output=stopover)
        shellout("""LC_ALL=C zgrep -v "^200" {input} >> {output}""",
                 input=self.input().path, output=stopover)
        output = shellout("sort -S50% -u {input} > {output}", input=stopover)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())
