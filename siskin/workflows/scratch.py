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
# Workflow experiments
#

from gluish.common import Executable
from gluish.utils import shellout
from siskin.sources.amsl import AMSLOpenAccessISSNList
from siskin.sources.jstor import JstorExport
from siskin.sources.doaj import DOAJExport
from siskin.task import DefaultTask
import luigi
import os
import tempfile

class ScratchTask(DefaultTask):
    TAG = 'scratch'

class ApplyOpenAccessFlag(ScratchTask):
    """
    Goal: Formeta + ISSN list = Formeta with OA flag.
    """
    def requires(self):
        return {
            'issn-list': AMSLOpenAccessISSNList(),
            'input': DOAJExport(format='formeta'),
        }

    def run(self):
        flux = """

        default fileName = "{input}";

        fileName|
        open-gzip|
        as-lines|
        decode-formeta|
        morph("{morph}")|
        encode-formeta|
        write("stdout");

        """.format(input=self.input().get('input').path, morph=self.assets('8986.xml'))
        
        with tempfile.NamedTemporaryFile(prefix='siskin-', delete=False) as temp:
            temp.write(flux)
            temp.flush()

        output = shellout("flux.sh {script} > {output}", script=temp.name)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())
