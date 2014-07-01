# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103

"""
IMDB.
"""

from gluish.common import FTPMirror
from gluish.intervals import hourly
from siskin.task import DefaultTask
import luigi

class IMDBTask(DefaultTask):
    TAG = 'imdb'

class IMDBSync(IMDBTask):

    indicator = luigi.Parameter(default=hourly(fmt='%s'))

    def requires(self):
        return FTPMirror(host='ftp.fu-berlin.de',
                         username='anonymous',
                         password='',
                         base='/pub/misc/movies/database/')

    def run(self):
        self.input().move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())
