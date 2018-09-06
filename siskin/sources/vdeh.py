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

External Java app required:

    $ sha1sum Mab2Mabxml-1.9.9.jar
    b37c4663fe6e4dcc55e71253266516e417ec9c44  Mab2Mabxml-1.9.9.jar

Configuration:

[vdeh]

input = /path/to/mab (ask RS)
Mab2Mabxml.jar = /path/to/local/copy.jar
index-url = http://localhost:8983/solr/biblio/select

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

    Requires Mab2Mabxml, downloadable from
    https://sourceforge.net/projects/dnb-conv-tools/.
    """

    def run(self):
        output = shellout("java -jar {jarfile} -i {input} -o {output}",
                          jarfile=self.config.get("vdeh", "Mab2Mabxml.jar"),
                          input=self.config.get("vdeh", "input"))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="xml"))

class VDEHRemoveIllegalChars(VDEHTask):
    """
    Remove Nichtsortierzeichen.

    https://stackoverflow.com/a/7774512, https://stackoverflow.com/a/36371662

    We want to remove unicode codepoint U+00AC (c2ac, o302 o254), aka the
    "NOT SIGN", aka the "Nichtsortierzeichen".

    When removing only o254, the dangling o302 causes problems (invalid
    continuation byte) - but not always.

    tr -d '[:cntrl:]' would not remove it, since c2ac starts with 0302,
    which is used for control chars as well (https://is.gd/GRIjmy).

    s/\u00AC\u00C2//g would not work for some reason.

    Anecdata: The stripped file is 57740 bytes smaller.
    """
    def requires(self):
        return VDEHXML()

    def run(self):
        output = shellout(r" sed -e $(echo -e 's/\o302\o254//g') < {input} > {output} ", input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))

class VDEHMARC(VDEHTask):
    """ Convert MABxml to BinaryMarc """

    def requires(self):
        return VDEHRemoveIllegalChars()

    def run(self):
        output = shellout("""python {script} {input} {output} {server}""",
                          script=self.assets("130/130_marcbinary.py"),
                          input=self.input().path,
                          server=self.config.get('vdeh', 'index-url'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='fincmarc.mrc'))
