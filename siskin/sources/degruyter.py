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
from gluish.intervals import daily
from gluish.utils import shellout
from siskin.configuration import Config
from siskin.task import DefaultTask
import luigi
import tempfile

config = Config.instance()

class DegruyterTask(DefaultTask):
	TAG = 'degryter'

class DegruyterPaths(DegruyterTask):
    """ A list of Degruyter ile paths (via FTP). """
    indicator = luigi.Parameter(default=daily().strftime('%s'))

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
        return luigi.LocalTarget(path=self.path(ext="filelist"), format=TSV)

class DegruyterXMLFiles(DegruyterTask):
    """ Extract all XML files from Degruyter dump. """

    def requires(self):
        return DegruyterPaths()

    @timed
    def run(self):
        stopover = tempfile.mkdtemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                shellout("unzip -qq -d {stopover} {path} *.xml 2> /dev/null", stopover=stopover, path=row.path,
                         ignoremap={1: '... but processing completed successfully anyway'})
        shutil.move(stopover, self.taskdir())
        with self.output().open('w') as output:
            for path in iterfiles(self.taskdir(), fun=lambda p: p.endswith('.xml')):
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="filelist"), format=TSV)

class DegruyterXMLCombine(DegruyterTask):
    """ Synthesize a single XML file from all article XML files. """

    def requires(self):
        return DegruyterXMLFiles()

    @timed
    def run(self):
        pass

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="xml"))
