#!/usr/bin/env python
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
Homepage: http://www.textgrid.de/ueber-textgrid/digitale-bibliothek/
License: http://creativecommons.org/licenses/by/3.0/de/
"""

from gluish.format import TSV
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.common import Directory
from siskin.task import DefaultTask
from siskin.utils import iterfiles
import datetime
import luigi
import os

class TextGridTask(DefaultTask):
    TAG = 'textgrid'

    def closest(self):
        return datetime.date(2014, 11, 1)

class TextGridDownload(TextGridTask):
    """ Download of various corpus dumps. """
    date = ClosestDateParameter(default=datetime.date.today())
    corpus = luigi.Parameter(default='all', description='all, v1, v2')

    def requires(self):
        return {'dir': Directory(path=os.path.join(self.taskdir(), self.corpus, str(self.closest())))}

    def run(self):
        filemap = {'all': 'http://www.textgrid.de/fileadmin/digitale-bibliothek/literatur.zip',
                   'v1': 'http://www.textgrid.de/fileadmin/digitale-bibliothek/literatur-nur-texte-1.zip',
                   'v2': 'http://www.textgrid.de/fileadmin/digitale-bibliothek/literatur-nur-texte-2.zip'}

        if self.corpus not in filemap:
            raise RuntimeError('available corpus ids: all, v1, v2')

        output = shellout("wget --retry-connrefused '{url}' -O {output}", url=filemap[self.corpus])
        shellout("unzip -d '{dir}' {input}", dir=self.input().get('dir').path, input=output)
        with self.output().open('w') as output:
            for path in iterfiles(self.input().get('dir').path):
                output.write_tsv(path.encode('utf-8'))

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
