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
https://www.ceeol.com/, refs #9398.

Currently, there are two XML formats used, a custom CEEOL XML and a slightly
invalid MARCXML. The last update was manually uploaded to a colorful server
@Telekom.

Config:

[ceeol]

disk-dir = /path/to/disk
updates = /path/to/file.xml, /path/to/another.xml, ...

"""

from __future__ import print_function

import glob
import os
import hashlib
import tempfile

import luigi
from gluish.utils import shellout

from siskin.task import DefaultTask


class CeeolTask(DefaultTask):
    """
    Base task.
    """
    TAG = '53'

    def updateid(self):
        """
        Only use the string of files as hashed identifier. TODO(miku): It would
        be more robust to use the checksums of all input files.
        """
        sha1 = hashlib.sha1()
        sha1.update(self.config.get("ceeol", "updates"))
        return sha1.hexdigest()

class CeeolJournalsUpdates(CeeolTask):
    """
    Create an intermediate schema from zero or more update XML (in MARCXML).
    The output will depend on the set and order of the input files.
    """
    def run(self):
        paths = [p.strip() for p in self.config.get("ceeol", "updates").split(",") if p.strip()]
        _, stopover = tempfile.mkstemp(prefix="siskin-")
        self.logger.debug("found %d updates", len(paths))
        for p in paths:
            shellout("span-import -i ceeol-marcxml {input} >> {output}", input=p, output=stopover)
        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        filename = "{}.ndj".format(self.updateid())
        return luigi.LocalTarget(path=self.path(filename=filename))

class CeeolJournalsIntermediateSchema(CeeolTask):
    """
    Combine all journals from disk dump and convert them to intermediate
    schema. Add updates. The output name of this task depends on the update id.

    This task depends on a fixed subdirectory, e.g. articles or 1Journal-Articles-XML..
    """
    def requires(self):
        return CeeolJournalsUpdates()

    def run(self):
        diskdir = self.config.get('ceeol', 'disk-dir')
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for path in glob.glob(os.path.join(diskdir, 'articles', 'articles_*xml')):
            shellout("span-import -i ceeol {input} | pigz -c >> {output}", input=path, output=stopover)
        shellout("cat {update} | pigz -c >> {output}", update=self.input().path, output=stopover)
        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        filename = "{}.ldj.gz".format(self.updateid())
        return luigi.LocalTarget(path=self.path(filename=filename))

