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
from gluish.format import TSV
from gluish.parameter import ClosestDateParameter
from gluish.path import iterfiles
from gluish.utils import shellout
from siskin.configuration import Config
from siskin.task import DefaultTask
import datetime
import luigi
import os
import tempfile

config = Config.instance()

class GBITask(DefaultTask):
    TAG = '048'

    def closest(self):
        return datetime.date(2015, 6, 1)

class GBIInventory(GBITask):
    """ Just a list of all files of a dump. """
    date = ClosestDateParameter(default=datetime.date.today())

    @timed
    def run(self):
        """ Hardcode directory for now.
        TODO(miku): replace this with ftp sync.
        """
        home = config.get('core', 'home', Config.NO_DEFAULT)
        directory = os.path.join(home, self.TAG, 'issue-5093')
        if not os.path.exists(directory):
            raise RuntimeError('dump dir not found: %s' % directory)
        with self.output().open('w') as output:
            for path in iterfiles(directory):
            output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GBIXMLCombined(GBITask):
    """ Extract all XML files and concat them. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
       return GBIInventory(date=self.date)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
            shellout("unzip -p {path} \*.xml 2> /dev/null >> {output}", output=stopover, path=row.path,
                 ignoremap={1: 'OK', 9: 'ignoring broken zip'})
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'), format=TSV)
