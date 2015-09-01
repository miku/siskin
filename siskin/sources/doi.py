# coding: utf-8

"""
International DOI Foundation (IDF), a not-for-profit membership organization
that is the governance and management body for the federation of Registration
Agencies providing DOI services and registration, and is the registration
authority for the ISO standard (ISO 26324) for the DOI system.
"""

from gluish.common import Executable
from gluish.intervals import yearly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.sources.crossref import CrossrefDOIList
from siskin.task import DefaultTask
import datetime
import luigi
import tempfile

class DOITask(DefaultTask):
    """
    A task for doi.org related things.
    """
    TAG = 'doi'

    def closest(self):
        return yearly(self.date)

class DOIHarvest(DOITask):
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
        """
        If we have more DOI sources, we could add them as requirements here.
        """
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

    1. A DOI redirects to http://www.crossref.org/deleted_DOI.html or
       most of these sites below crossref.org: https://gist.github.com/miku/6d754104c51fb553256d
    2. A DOI API lookup does not return a HTTP 200.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return DOIHarvest(date=self.date)

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout("""LC_ALL=C zgrep -E "http(s)?://.*.crossref.org" {input} >> {output}""",
                 input=self.input().path, output=stopover)
        shellout("""LC_ALL=C zgrep -v "^200" {input} >> {output}""",
                 input=self.input().path, output=stopover)
        output = shellout("sort -S50% -u {input} | cut -f4 | sed s@http://doi.org/api/handles/@@g > {output}", input=stopover)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())
