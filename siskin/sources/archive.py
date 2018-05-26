# coding: utf-8
# pylint: disable=C0301,E1101

# Copyright 2017 by Leipzig University Library, http://ub.uni-leipzig.de
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
archive.org, refs #8000
"""

import datetime

import luigi
from gluish.common import Executable
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.task import DefaultTask


class ArchiveTask(DefaultTask):
    TAG = "153"

    def closest(self):
        return monthly(date=self.date)


class ArchiveDownload(ArchiveTask):
    """
    OAI harvest.
    """

    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        url = "http://archive.org/services/oai.php"
        collection = "collection:prelinger"
        shellout("""metha-sync -set {collection} {url}""",
                 collection=collection, url=url)
        output = shellout("""metha-cat -set {collection} {url} > {output}""",
                          url=url, collection=collection)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="xml"))


class ArchiveMARC(ArchiveTask):
    """
    Convert.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return ArchiveDownload(date=self.date)

    def run(self):
        output = shellout("python {script} {input} {output}",
                          script=self.assets("153/153_marcbinary.py"),
                          input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='fincmarc.mrc'))


class ArchiveSearch(ArchiveTask):
    """
    Search archive via the ia tool. Requires Archive.org account:

    https://archive.org/account/login.createaccount.php

    The command `ia configure` will set you up.

        $ cat ~/.config/ia.ini
        [s3]
        access = asudiasd77xsdlds
        secret = oasdu888s8x9a0sd

        [cookies]
        logged-in-user = user@example.com
        logged-in-sig = secret

    Refs #8000.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    query = luigi.Parameter(default='collection:prelinger')

    def requires(self):
        """
        The setup of siskin should install this automatically.
        """
        return Executable(name='ia',
                          message='https://pypi.python.org/pypi/internetarchive')

    def run(self):
        output = shellout("ia search '{query}' > {output}", query=self.query)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj', digest=True))


class ArchiveSearchMetadata(ArchiveTask):
    """
    For a given search query, harvest all metadata. This can take a long time,
    since each record is requested separately. Maybe there is some faster way.

    Currently 1 record/s.

    Refs #8000.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    query = luigi.Parameter(default='collection:prelinger')

    def requires(self):
        return ArchiveSearch(date=self.date, query=self.query)

    def run(self):
        output = shellout("for i in $(jq -r .identifier {input}); do ia metadata $i; done >> {output}",
                          input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj', digest=True))


class ArchiveTelevisionTexts(ArchiveTask, luigi.WrapperTask):
    """
    Temporary task for harvesting special query from archive, refs #8000.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        """
        loans__status__status:NULL (as in web) would not work.
        """
        query = 'subject:television AND mediatype:texts AND -loans__status__status:"AVAILABLE" AND -loans__status__status:"UNAVAILABLE"'
        return ArchiveSearchMetadata(date=self.date, query=query)

    def run(self):
        luigi.LocalTarget(self.input().path).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj', digest=True))
