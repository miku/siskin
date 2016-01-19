# coding: utf-8

"""
Twarc harvested data.
"""

from gluish.utils import shellout
from siskin.configuration import Config
from siskin.task import DefaultTask
import datetime
import luigi
import os

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
        return luigi.LocalTarget(path=self.path(ext='filelist'))
