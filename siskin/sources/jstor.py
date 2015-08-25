# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301

"""
[jstor]

ftp-host = host.name
ftp-username = username
ftp-password = password
ftp-path = /
ftp-pattern = *
"""

from gluish.benchmark import timed
from gluish.common import FTPMirror, Executable
from gluish.format import TSV
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout, nwise
from siskin.configuration import Config
from siskin.task import DefaultTask
import datetime
import itertools
import luigi
import pipes
import tempfile

config = Config.instance()

class JstorTask(DefaultTask):
    """ Jstor base. """
    TAG = 'jstor'

    def closest(self):
        return weekly(self.date)

class JstorPaths(JstorTask):
    """ Sync. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return FTPMirror(host=config.get('jstor', 'ftp-host'),
                         username=config.get('jstor', 'ftp-username'),
                         password=config.get('jstor', 'ftp-password'),
                         pattern=config.get('jstor', 'ftp-pattern'))

    def run(self):
        self.input().move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class JstorMembers(JstorTask):
    """ Extract a full list of archive members. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return JstorPaths(date=self.date)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                shellout(""" unzip -l {input} | grep "xml$" | awk '{{print "{input}\t"$4}}' >> {output} """,
                         preserve_whitespace=True, input=row.path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class JstorLatestMembers(JstorTask):
    """ Find the latest archive members for all article ids. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return JstorMembers(date=self.date)

    @timed
    def run(self):
        """ Expect input to be sorted, so tac will actually be a perfect rewind. """
        output = shellout("tac {input} | sort -S 50% -u -k2,2 | sort -S 50% -k1,1 > {output}", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class JstorXML(JstorTask):
    """
    Create an latest snapshot of the latest data.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    batch = luigi.IntParameter(default=512, significant=False)

    def requires(self):
        return JstorLatestMembers(date=self.date)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            groups = itertools.groupby(handle.iter_tsv(cols=('archive', 'member')), lambda row: row.archive)
            for archive, items in groups:
                for chunk in nwise(items, n=self.batch):
                    margs = " ".join(["'%s'" % item.member.replace('[', r'\[').replace(']', r'\]') for item in chunk])
                    shellout("unzip -qq -c {archive} {members} >> {output}", archive=archive, members=margs, output=stopover)

        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'), format=TSV)

class JstorIntermediateSchema(JstorTask):
    """ Convert to intermediate format via span. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
       return {'span': Executable(name='span-import', message='http://git.io/vI8NV'),
               'file': JstorXML(date=self.date)}

    @timed
    def run(self):
        output = shellout("span-import -i jstor {input} > {output}", input=self.input().get('file').path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class JstorISSNList(JstorTask):
    """ A list of JSTOR ISSNs. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return JstorIntermediateSchema(date=self.date)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout("""jq -r '.["rft.issn"][]' {input} 2> /dev/null >> {output} """, input=self.input().path, output=stopover)
        shellout("""jq -r '.["rft.eissn"][]' {input} 2> /dev/null >> {output} """, input=self.input().path, output=stopover)
        output = shellout("""sort -u {input} > {output} """, input=stopover)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class JstorDOIList(JstorTask):
    """ A list of JSTOR DOIs. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return JstorIntermediateSchema(date=self.date)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout("""jq -r '.doi' {input} | grep -v null >> {output} """, input=self.input().path, output=stopover)
        output = shellout("""sort -u {input} > {output} """, input=stopover)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
