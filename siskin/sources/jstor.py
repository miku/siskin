# coding: utf-8

"""
[jstor]

ftp-host = host.name
ftp-username = username
ftp-password = password
ftp-path = /
ftp-pattern = *
"""

from gluish.benchmark import timed
from gluish.common import FTPMirror
from gluish.format import TSV
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.path import iterfiles
from gluish.utils import shellout
from siskin.configuration import Config
from siskin.task import DefaultTask
import datetime
import luigi
import re
import shutil
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

class JstoreXMLFiles(JstorTask):
    """ Extract all XML files from Jstor dump. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return JstorPaths(date=self.date)

    @timed
    def run(self):
        stopover = tempfile.mkdtemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                shellout("unzip -qq -d {stopover} {path} *.xml 2> /dev/null", stopover=stopover, path=row.path, ignoremap={1: 'OK'})
        shutil.move(stopover, self.taskdir())
        with self.output().open('w') as output:
            for path in iterfiles(self.taskdir(), fun=lambda p: p.endswith('.xml')):
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="filelist"), format=TSV)

class JstoreXMLCombine(JstorTask):
    """ Synthesize a single XML file from all article XML files. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return JstoreXMLFiles(date=self.date)

    @timed
    def run(self):
        docp = re.compile(r"<!DOCTYPE[^>]*>", re.IGNORECASE | re.MULTILINE)
        decp = re.compile(r"<\?xml[^>]*>", re.IGNORECASE | re.MULTILINE)
        with self.output().open('w') as output:
            output.write('<synthetic-root>')
            with self.input().open() as handle:
                for row in handle.iter_tsv(cols=('path',)):
                    with open(row.path) as fh:
                        content = fh.read()
                        content = docp.sub("", content)
                        content = decp.sub("", content)
                        output.write(content)
            output.write('</synthetic-root>')

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="xml"))
