# coding: utf-8

"""
Things that should run often.
"""

from gluish.utils import random_string
from siskin.task import DefaultTask
import datetime
import luigi

class Workflow(DefaultTask):
    TAG = 'workflows'

class DailyIndexUpdates(Workflow):
    """
    A wrapper around a lot of heavy tasks to keep ES up to date.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    indicator = luigi.Parameter(default=random_string())

    def requires(self):
        from siskin.sources.bms import BMSIndex
        from siskin.sources.bnf import BNFIndex
        from siskin.sources.bsz import BSZIndex
        from siskin.sources.disson import DissonIndex
        from siskin.sources.doab import DOABIndex
        from siskin.sources.ebl import EBLIndex
        from siskin.sources.elsevier import ElsevierIndex
        from siskin.sources.ema import EMAIndex
        from siskin.sources.gbv import GBVIndex
        from siskin.sources.hszigr import HSZIGRIndex
        from siskin.sources.imslp import IMSLPIndex
        from siskin.sources.ksd import KSDIndex
        from siskin.sources.lfer import LFERIndex
        from siskin.sources.mor import MORIndex
        from siskin.sources.mtc import MTCIndex
        from siskin.sources.naxos import NaxosIndex
        from siskin.sources.nep import NEPIndex
        from siskin.sources.nl import NLIndex
        from siskin.sources.oso import OSOIndex
        from siskin.sources.pao import PAOIndex
        from siskin.sources.qucosa import QucosaIndex
        from siskin.sources.rism import RISMIndex

        return [
            BMSIndex(date=self.date),
            BNFIndex(date=self.date),
            BSZIndex(end=self.date, indicator=self.indicator),
            DissonIndex(date=self.date),
            DOABIndex(date=self.date),
            EBLIndex(date=self.date),
            ElsevierIndex(date=self.date),
            EMAIndex(date=self.date),
            GBVIndex(date=self.date),
            HSZIGRIndex(date=self.date),
            IMSLPIndex(date=self.date),
            KSDIndex(date=self.date),
            LFERIndex(date=self.date),
            MORIndex(date=self.date),
            MTCIndex(date=self.date),
            NaxosIndex(date=self.date),
            NEPIndex(date=self.date),
            NLIndex(date=self.date),
            OSOIndex(date=self.date),
            PAOIndex(date=self.date),
            QucosaIndex(date=self.date),
            RISMIndex(date=self.date),
        ]

    def run(self):
        self.logger.debug("Synced {0} indices on {1}".format(len(self.input()), self.date))
        with self.output().open('w'):
            pass

    def output(self):
        return luigi.LocalTarget(path=self.path())
