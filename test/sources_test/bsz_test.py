# coding: utf-8

from gluish.format import TSV
from gluish.task import BaseTask
from siskin.sources.bsz import UnifiedSnapshot
import luigi
import os
import tempfile
import unittest
import random
import datetime

FIXTURES = os.path.join(os.path.dirname(__file__), '..', 'fixtures')

class DefaultTestTask(BaseTask):
    BASE = tempfile.gettempdir()
    TAG = 'siskin-tests'

class SnapshotBasicMock0010(luigi.ExternalTask):
    def output(self):
        return luigi.LocalTarget(path=os.path.join(FIXTURES, 'SB-0010-100.tsv'), format=TSV)

class SnapshotBasicMock0020(luigi.ExternalTask):
    def output(self):
        return luigi.LocalTarget(path=os.path.join(FIXTURES, 'SB-0020-100.tsv'), format=TSV)

random_id = lambda: ''.join(map(str, [random.randint(0, 9) for _ in range(10)]))

class UnifiedSnapshotMock(UnifiedSnapshot, DefaultTestTask):
    begin = luigi.DateParameter(default=datetime.date(2000, 1, 1))
    end = luigi.DateParameter(default=datetime.date(2000, 1, 1))
    test_indicator = luigi.IntParameter(default=random_id())
    
    def requires(self):
        return [SnapshotBasicMock0010(), SnapshotBasicMock0020()]

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class UnifiedSnapshotTest(unittest.TestCase):
    def test_unified_snapshot(self):
        task = UnifiedSnapshotMock()
        luigi.build([task], local_scheduler=True)
        print(task.output().path)
        with task.output().open() as handle:
            lines = handle.readlines()
            self.assertEquals(186, len(lines))
