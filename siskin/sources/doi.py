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
International DOI Foundation (IDF), a not-for-profit membership organization
that is the governance and management body for the federation of Registration
Agencies providing DOI services and registration, and is the registration
authority for the ISO standard (ISO 26324) for the DOI system.
"""

from gluish.common import Executable
from gluish.intervals import yearly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.sources.crossref import CrossrefDOIList
from siskin.task import DefaultTask
import datetime
import luigi
import tempfile

class DOITask(DefaultTask):
    """
    A task for doi.org related things.
    """
    TAG = 'doi'

    def closest(self):
        return yearly(self.date)

class DOIHarvest(DOITask):
    """
    Harvest DOI redirects from doi.org API.

    ---- FYI ----

    It is highly recommended that you put a static entry into /etc/hosts
    for doi.org while `hurrly` is running.

    As of 2015-07-31 doi.org resolves to six servers. Just choose one.

        $ nslookup doi.org

    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        """
        If we have more DOI sources, we could add them as requirements here.
        """
        return {'input': CrossrefDOIList(date=self.date),
                'hurrly': Executable(name='hurrly', message='http://github.com/miku/hurrly'),
                'pigz': Executable(name='pigz', message='http://zlib.net/pigz/')}

    def run(self):
        output = shellout("hurrly -w 64 < {input} | pigz > {output}", input=self.input().get('input').path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='tsv.gz'))

class DOIBlacklist(DOITask):
    """
    Create a blacklist of DOIs. Possible cases:

    1. A DOI redirects to http://www.crossref.org/deleted_DOI.html or
       most of these sites below crossref.org: https://gist.github.com/miku/6d754104c51fb553256d
    2. A DOI API lookup does not return a HTTP 200.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return DOIHarvest(date=self.date)

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout("""LC_ALL=C zgrep -E "http(s)?://.*.crossref.org" {input} >> {output}""",
                 input=self.input().path, output=stopover)
        shellout("""LC_ALL=C zgrep -v "^200" {input} >> {output}""",
                 input=self.input().path, output=stopover)
        output = shellout("sort -S50% -u {input} | cut -f4 | sed s@http://doi.org/api/handles/@@g > {output}", input=stopover)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())
