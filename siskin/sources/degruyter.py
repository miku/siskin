# coding: utf-8

"""
DeGruyter task.

[degruyter]

ftp-host = host.name
ftp-username = username
ftp-password = password
ftp-path = /
ftp-pattern = some*glob*pattern.zip

"""

from gluish.benchmark import timed
from gluish.common import FTPMirror
from gluish.format import TSV
from gluish.intervals import hourly
from siskin.configuration import Config
from siskin.task import DefaultTask
import luigi

config = Config.instance()

class DegruyterTask(DefaultTask):
	TAG = 'degryter'

class DegruyterPaths(DegruyterTask):
    """ A list of Degruyter ile paths (via FTP). """
    indicator = luigi.Parameter(default=hourly(fmt='%s'))

    def requires(self):
        host = config.get('degruyter', 'ftp-host')
        username = config.get('degruyter', 'ftp-username')
        password = config.get('degruyter', 'ftp-password')
        base = config.get('degruyter', 'ftp-path')
        pattern = config.get('degruyter', 'ftp-pattern')
        return FTPMirror(host=host, username=username, password=password,
                         base=base, pattern=pattern)

    @timed
    def run(self):
        self.input().move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
