# coding: utf-8
# pylint: disable=C0301

# Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
#                   Martin Czygan, <martin.czygan@uni-leipzig.de>
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

#
# Adhoc
# =====
#

import luigi

from gluish.format import TSV
from siskin.sources.crossref import CrossrefExport
from siskin.sources.degruyter import DegruyterExport
from siskin.sources.doaj import DOAJExport
from siskin.sources.elsevierjournals import ElsevierJournalsExport
from siskin.sources.highwire import HighwireExport
from siskin.sources.jstor import JstorExport
from siskin.task import DefaultTask


class AdhocTask(DefaultTask):
    """
    Base task for throwaway tasks.
    """
    TAG = "adhoc"


class AdhocFormetaSamples(AdhocTask):
    """
    Formeta test samples.
    """

    def requires(self):
        return [
            CrossrefExport(format='formeta'),
            DegruyterExport(format='formeta'),
            DOAJExport(format='formeta'),
            ElsevierJournalsExport(format='formeta'),
            HighwireExport(format='formeta'),
            JstorExport(format='formeta'),
        ]

    def run(self):
        with self.output().open('w') as output:
            for target in self.input():
                output.write_tsv(target.path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
