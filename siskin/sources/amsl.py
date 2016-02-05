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
Electronic Resource Management System Based on Linked Data Technologies.

http://amsl.technology

Config:

[amsl]

isil-rel = https://x.com/static/about.json
holdings = https://x.com/inhouseservices/list?do=holdings
uri-download-prefix = https://x.y.z/OntoWiki/files/get?setResource=

"""

from gluish.utils import shellout
from siskin.configuration import Config
from siskin.task import DefaultTask
import datetime
import json
import luigi
import tempfile
import zipfile

config = Config.instance()

class AMSLTask(DefaultTask):
    TAG = 'amsl'

class AMSLCollections(AMSLTask):
    """ Get a list of ISIL, source and collection choices via JSON API. """

    date = luigi.DateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("""curl --fail "{link}" > {output} """, link=config.get('amsl', 'isil-rel'))
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

class AMSLHoldings(AMSLTask):
    """
    Download AMSL tasks.
    """
    date = luigi.Parameter(default=datetime.date.today())

    def run(self):
        output = shellout(""" curl --fail "{link}" > {output} """, link=config.get('amsl', 'holdings'))
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

class AMSLHoldingsISILList(AMSLTask):
    """
    Return a list of ISILs that are returned by the API.
    """
    date = luigi.Parameter(default=datetime.date.today())

    def requires(self):
        return AMSLHoldings(date=self.date)

    def run(self):
        output = shellout("jq -r '.[].ISIL' {input} | sort > {output}", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

class AMSLHoldingsFile(AMSLTask):
    """
    Access AMSL files/get?setResource= facilities.

    The output is probably zipped.

    One ISIL can have multiple files.

    If the file is Zipped, it will be decompressed.
    """
    isil = luigi.Parameter(description='ISIL, case sensitive')
    date = luigi.Parameter(default=datetime.date.today())

    def requires(self):
        return AMSLHoldings(date=self.date)

    def run(self):
        with self.input().open() as handle:
            holdings = json.load(handle)

        _, stopover = tempfile.mkstemp('siskin-')

        for holding in holdings:
            if holding["ISIL"] == self.isil:
                link = "%s%s" % (config.get('amsl', 'uri-download-prefix'), holding['DokumentURI'])
                downloaded = shellout("curl --fail {link} > {output} ", link=link)
                try:
                    _ = zipfile.ZipFile(downloaded)
                    output = shellout("unzip -p {input} >> {output}", input=downloaded, output=stopover)
                except zipfile.BadZipfile:
                    # at least the file is not a zip.
                    output = shellout("cat {input} >> {output}", input=downloaded, output=stopover)

        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())
