# coding: utf-8
#
# Copyright 2018 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
#                   Martin Czygan, <martin.czygan@uni-leipzig.de>
#                   Robert Schenk, <robert.schenk@uni-leipzig.de>#                   
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
VDEH, refs #9868.

Note: These tasks are based on an MABxml version of the original binary MAB.

Configuration:

[vdeh]

input = /path/to/mab (ask RS)
"""

import luigi
from gluish.utils import shellout

from siskin.task import DefaultTask


class VDEHTask(DefaultTask):
    """
    This task inherits functionality from `siskin.task.DefaultTask`.
    """
    TAG = '130'

class VDEHXML(VDEHTask):
    """
    Convert binary MAB to MABxml.
    """

    def run(self):        
        output = shellout("java -jar dnbtools.jar - Mab2Mabxml -{input} -{output}",
                          input=self.config.get("VDEH", "input"))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="xml"))

class VDEHMARC(VDEHTask):
    """ Convert MABxml to BinaryMarc """

    def requires(self):
        return VDEHXML()

    def run(self):
        output = shellout("""python {script} {input} {output}""",
                          script=self.assets("130/130_marcbinary.py"),
                          input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='fincmarc.mrc'))