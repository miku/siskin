# coding: utf-8
#
# Copyright 2017 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
#                   Robert Schenk, <robert.schenk@uni-leipzig.de>
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
Filmmusikforschung (FMF), refs #7967.

Note: These tasks are based on an XML version of the original XLSX, you need to
configure that.

Configuration:

[kielfmf]

input = /path/to/xml (ask RS)
"""

import luigi
from gluish.utils import shellout
from luigi.format import Gzip

from siskin.task import DefaultTask


class KielFMFTask(DefaultTask):
    """
    This task inherits functionality from `siskin.task.DefaultTask`.
    """
    TAG = '101'


class KielFMFIntermediateSchema(KielFMFTask):
    """
    Create intermediate schema.
    """

    def run(self):
        # We need an XML file. Uee a config entry to point to that file.
        input = self.config.get('kielfmf', 'input')

        # shellout run shell command and allows placeholders. The {output}
        # placeholder is a bit special. If it is not set explicitly, it will
        # point to a temporary file. The return value of shellout is the value of
        # output.
        output = shellout("""
            flux.sh {flux} inputfile={input} | pigz -c > {output}
        """, flux=self.assets('101/101_flux.flux'), input=input)

        # This moves the output of the above shell command into the desired task
        # output path.
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)
