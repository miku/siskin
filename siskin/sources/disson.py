# coding: utf-8
# pylint: disable=C0301,E1101

# Copyright 2018 by Leipzig University Library, http://ub.uni-leipzig.de
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
Disson, Dissertations Online, refs #3422.
"""

import datetime

import luigi

from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask


class DissonTask(DefaultTask):
    """
    Base task for disson.
    """
    TAG = '13'

    def closest(self):
        return monthly(date=self.date)


class DissonHarvest(DissonTask):
    """
    Via OAI, access might be restricted.

    ;; ANSWER SECTION:
    services.dnb.de.	3539	IN	CNAME	httpd-e02.dnb.de.
    httpd-e02.dnb.de.	3539	IN	CNAME	prodche-02.dnb.de.
    prodche-02.dnb.de.	3539	IN	A	    193.175.100.222

    Format options (2018-03-02):

    * baseDc-xml
    * BibframeRDFxml
    * JATS-xml
    * MARC21plus-1-xml
    * MARC21plus-xml
    * MARC21-xml
    * mods-xml
    * oai_dc
    * ONIX-xml
    * PicaPlus-xml
    * RDFxml
    * sync-repo-xml
    * xMetaDiss

    """
    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='MARC21-xml')
    set = luigi.Parameter(default='dnb-all:online:dissertations')

    def run(self):
        endpoint = "http://services.dnb.de/oai/repository"
        shellout("metha-sync -daily -format {format} -set {set} {endpoint}",
                 format=self.format, set=self.set, endpoint=endpoint)
        output = shellout("""metha-cat -format {format} -set {set} {endpoint} |
                             pigz -c > {output}""", format=self.format, set=self.set,
                          endpoint=endpoint)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml.gz', digest=True))


class DissonMARC(DissonTask):
    """
    Create a binary MARC version.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return DissonHarvest(date=self.date)

    def run(self):
        output = shellout("yaz-marcdump -i marcxml -o marc <(unpigz -c {input}) > {output}",
                          input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))
