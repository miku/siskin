# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301

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

"""
Pubmed PMC FTP.
"""

from __future__ import print_function

import datetime

import luigi
from gluish.format import TSV
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.benchmark import timed
from siskin.common import FTPMirror
from siskin.task import DefaultTask


class PubmedTask(DefaultTask):
    """ Pubmed base. """
    TAG = 'pubmed'

    def closest(self):
        return weekly(self.date)


class PubmedMetadataPaths(PubmedTask):
    """
    Sync metadata only (much faster).
    """
    date = ClosestDateParameter(default=datetime.date.today())
    max_retries = luigi.IntParameter(default=10, significant=False)
    timeout = luigi.IntParameter(
        default=20, significant=False, description='timeout in seconds')

    def requires(self):
        return FTPMirror(host='ftp.ncbi.nlm.nih.gov',
                         base='/pub/pmc/',
                         pattern='articles*tar.gz',
                         max_retries=self.max_retries,
                         timeout=self.timeout)

    @timed
    def run(self):
        self.input().move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class PubmedJournalList(PubmedTask):
    """
    Download journal list.
    """

    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout(
            """curl --fail "http://www.ncbi.nlm.nih.gov/pmc/journals/collections/?format=csv" > {output} """)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())


