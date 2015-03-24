# coding: utf-8

"""
[jstor]

ftp-host = host.name
ftp-username = username
ftp-password = password
ftp-path = /
ftp-pattern = *
"""

from gluish.common import FTPMirror
from gluish.intervals import daily
from gluish.parameter import ClosestDateParameter
from siskin.task import DefaultTask
from siskin.configuration import Config
import datetime
import luigi

config = Config.instance()

class JstorTask(DefaultTask):
    """ Jstor base. """
    TAG = 'jstor'

    def closest(self):
        return daily(self.date)

class JstorSync(JstorTask):
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
        return luigi.LocalTarget(path=self.path())
