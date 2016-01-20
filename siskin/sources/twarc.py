# coding: utf-8

"""
Twarc harvested data.
"""

from gluish.format import TSV
from gluish.utils import shellout
from siskin.configuration import Config
from siskin.task import DefaultTask
from siskin.utils import iterfiles
import datetime
import json
import luigi
import os
import tempfile

config = Config.instance()

class TwarcTask(DefaultTask):
    TAG = 'twarc'

class TwarcSync(TwarcTask):
    """
    Sync data from harvester.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def run(self):
        target = os.path.join(self.taskdir(), 'mirror')
        if not os.path.exists(target):
            os.makedirs(target)
        shellout("rsync -avzP {remote} {target}", remote=config.get('twarc', 'remote'), target=target)
        with self.output().open('w') as output:
            for path in iterfiles(target):
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)

class TwarcChecksums(TwarcTask):
    """
    Calculate the checksum for each twarc file.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return TwarcSync(date=self.date)

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='twarc-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                shellout("md5sum {input} >> {output}", input=row.path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'))

class TwarcURLList(TwarcTask):
    """
    For a archive, identified by its checksum, extract all URLs.
    """
    checksum = luigi.Parameter()

    def requires(self):
        return TwarcChecksums()

    def run(self):
        archive = None

        with self.input().open() as handle:
            for line in handle:
                checksum, path = line.strip().split()
                if checksum == self.checksum:
                    archive = path

        if archive is None:
            raise RuntimeError('no archive with this checksum: %s' % self.checksum)

        with self.output().open('w') as output:
            with open(archive) as handle:
                for row in handle:
                    try:
                        tweet = json.loads(row)
                        for obj in tweet.get('entities', {}).get('urls', []):
                            output.write_tsv(obj.get('url').encode('utf-8'), obj.get('expanded_url').encode('utf-8'))
                    except Exception as err:
                        self.logger.warn(err)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class TwarcURLListWrapper(TwarcTask, luigi.WrapperTask):

    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        prerequisite = TwarcChecksums()
        luigi.build([prerequisite])
        with prerequisite.output().open() as handle:
            for line in handle:
                checksum, path = line.strip().split()
                if not path.endswith('json'):
                    continue
                yield TwarcURLList(checksum=checksum)

    def output(self):
        return self.input()

class TwarcURLs(TwarcTask):

    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return TwarcURLListWrapper(date=self.date)

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
