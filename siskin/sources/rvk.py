#!/usr/bin/env python
# coding: utf-8
#
#  Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                 by The Finc Authors, http://finc.info
#                 by Martin Czygan, <martin.czygan@uni-leipzig.de>
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

"""
Configuration keys:

[rvk]

url = http://example.org/path/to/rvk.zip
"""

from gluish.format import TSV
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.configuration import Config
from siskin.task import DefaultTask
from siskin.utils import iterfiles
import collections
import datetime
import luigi
import operator
import os
import tempfile

config = Config.instance()

class RVKTask(DefaultTask):
    """ Default RVK task. """
    TAG = 'rvk'

    def closest(self):
        return datetime.date(2014, 11, 1)

class RVKDownload(RVKTask):
    """ Download and unzip XML dump of RVK. """
    url = luigi.Parameter(default=config.get('rvk', 'url', 'http://example.org/path/to/rvk.zip'))
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        target = os.path.join(self.taskdir(), str(self.date))
        if not os.path.exists(target):
            os.makedirs(target)
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout("wget --retry-connrefused -O {stopover} '{url}' && unzip -o -d {dir} {stopover}", dir=target, stopover=stopover, url=self.url)
        files = list(iterfiles(target))
        if not len(files) == 1:
            raise RuntimeError('more than one file')
        luigi.File(files[0]).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml', digest=True))

class RVKPaths(RVKTask):
    """ Output path to root, one path per line. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return RVKDownload(date=self.date)

    def run(self):
        output = shellout("xsltproc {xsl} {path} > {output}", xsl=self.assets('rvk.xsl'), path=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

class RVKNames(RVKTask):
    """ Output path to root, one path per line. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return RVKDownload(date=self.date)

    def run(self):
        output = shellout("xsltproc {xsl} {path} > {output}", xsl=self.assets('rvktsv.xsl'), path=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class RVKBSZList(RVKTask):
    """ Extract PPN/RVK from current BSZ index. """

    date = luigi.Parameter(default=datetime.date.today())

    def run(self):
        output = shellout("""estab -indices bsz -f "_id content.936.a" > {output} """)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class RVKBSZDistribution(RVKTask):
    """ How many classes are specified. """
    date = luigi.Parameter(default=datetime.date.today())

    def requires(self):
        return RVKBSZList(date=self.date)

    def run(self):
        freq = collections.defaultdict(int)
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('ppn', 'rvks')):
                for rvk in row.rvks.split('|'):
                    freq[rvk] += 1

        with self.output().open('w') as output:
            for rvk, count in sorted(freq.iteritems(), key=operator.itemgetter(1), reverse=True):
                output.write_tsv(rvk, count)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
