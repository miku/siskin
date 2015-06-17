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
from gluish.common import FTPMirror, Executable
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

class JstorXML(JstorTask):
    """ Extract all XML files from Jstor dump. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return JstorPaths(date=self.date)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                shellout("unzip -p {path} \*.xml 2> /dev/null >> {output}", output=stopover, path=row.path, ignoremap={1: 'OK'})
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
