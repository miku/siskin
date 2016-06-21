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
IEEE.
"""

import luigi
from gluish.utils import shellout

from siskin.task import DefaultTask


class IEEETask(DefaultTask):
    TAG = 'ieee'

class IEEEIntermediateSchema(IEEETask):
    """
    Brute-convert any XML that has an ISSN.

    Dump style:

        $ ls -1 /tmp/uol/
        IEEcnf
        IEEEcnf
        IEEEper
        IEEEstd
        IEEper
        $RECYCLE.BIN
        System Volume Information

    E.g. taskdo IEEEIntermediateSchema --dir /tmp/uol/IEEEstd

    """
    dir = luigi.Parameter(description='directory to traverse')

    def run(self):
        output = shellout("""find {dir} -name "*xml" -type f | xargs -I {{}} span-import -i ieee {{}} | pigz -c >> {output}""", dir=self.dir)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True, ext='ldj.gz'))
