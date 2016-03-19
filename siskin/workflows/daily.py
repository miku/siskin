# coding: utf-8

#  Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                    The Finc Authors, http://finc.info
#                    Martin Czygan, <martin.czygan@uni-leipzig.de>
#
# This file is part of some open source application.
#
# Some open source application is free software: you can redistribute
# it and/or modify it under the terms of the GNU General Public
# License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.
#
# Some open source application is distributed in the hope that it will
# be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
#
# @license GPL-3.0+ <http://spdx.org/licenses/GPL-3.0+>

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
