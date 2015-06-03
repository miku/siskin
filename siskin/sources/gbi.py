#!/usr/bin/env python
# coding: utf-8

"""
GBI publisher.

[gbi]

ftp-host = host.name
ftp-username = username
ftp-password = password
ftp-path = /
ftp-pattern = some*glob*pattern.zip
"""

from gluish.benchmark import timed
from gluish.common import FTPMirror
from gluish.format import TSV
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.configuration import Config
from siskin.task import DefaultTask
import datetime
import luigi
import tempfile

config = Config.instance()

class GBITask(DefaultTask):
    TAG = '048'

    def closest(self):
        return datetime.date(2015, 6, 1)

class GBISync(GBITask):
    """ Sync. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return FTPMirror(host=config.get('gbi', 'ftp-host'),
                         username=config.get('gbi', 'ftp-username'),
                         password=config.get('gbi', 'ftp-password'),
                         pattern=config.get('gbi', 'ftp-pattern'))

    @timed
    def run(self):
        self.input().move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GBIXMLCombined(GBITask):
    """ Extract all XML files and concat them. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
       return GBISync(date=self.date)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                shellout("""unzip -p {path} \*.xml 2> /dev/null |
                            iconv -f iso-8859-1 -t utf-8 |
                            LC_ALL=C grep -v "^<\!DOCTYPE GENIOS PUBLIC" |
                            LC_ALL=C sed -e 's@<?xml version="1.0" encoding="ISO-8859-1" ?>@@g' >> {output}""",
                         output=stopover, path=row.path, ignoremap={1: 'OK', 9: 'ignoring broken zip'})
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'), format=TSV)
