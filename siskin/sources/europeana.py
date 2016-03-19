# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

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
Europeana NT.
"""

from gluish.format import TSV
from gluish.utils import shellout
from siskin.benchmark import timed
from siskin.task import DefaultTask
from siskin.utils import iterfiles
import glob
import luigi
import os
import shutil
import tempfile

class EuropeanaTask(DefaultTask):
    TAG = 'europeana'

class EuropeanaDownload(EuropeanaTask):
    """ Download Europeana dump. """
    base = luigi.Parameter(default='http://data.europeana.eu/download', significant=False)
    version = luigi.Parameter(default='2.0')
    format = luigi.Parameter(default='nt')

    @timed
    def run(self):
        target = os.path.join(self.taskdir(), self.version, self.format)
        if not os.path.exists(target):
            os.makedirs(target)
        url = os.path.join(self.base, self.version, "datasets", self.format)
        stopover = tempfile.mkdtemp(prefix='siskin-')
        shellout("""wget -q -nd -P {directory} -rc -np -A.{format}.gz '{url}'""",
                    url=url, directory=stopover, format=self.format)
        for path in glob.glob(unicode(os.path.join(stopover, '*'))):
            dst = os.path.join(target, os.path.basename(path))
            if not os.path.exists(dst):
                # this is atomic given path and target are on the same device
                shutil.move(path, target)
        with self.output().open('w') as output:
            for path in iterfiles(target, fun=lambda p: p.endswith('nt.gz')):
                output.write_tsv(self.version, self.format, path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)
