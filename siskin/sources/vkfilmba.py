# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301

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
#

"""
VKFilm Bundesarchiv, #8695.

TODO: Unify VKFilm* tasks.

Config
------

[vkfilmba]

baseurl = https://example.com

data = http://example.com/data.zip
deletions = http://example.com/del.txt

"""

import datetime
import os
import re
import shutil
import tempfile
import zipfile

import luigi

import pymarc
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
from siskin.utils import iterfiles


class VKFilmBATask(DefaultTask):
    """
    Bundesarchiv.
    """
    TAG = '148'

    def closest(self):
        return weekly(date=self.date)


class VKFilmBADownload(VKFilmBATask):
    """
    Download raw data daily. At least try, files may be missing, refs #12460.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def run(self):
        """
        XXX: Maybe ensure zip.
        """
        output = shellout("""curl --fail "{url}" > {output} """,
                          url=self.config.get('vkfilmba', 'data'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        """
        We expect zip, but to guarantee.
        """
        return luigi.LocalTarget(path=self.path(ext="zip"))


class VKFilmBADownloadDeletions(VKFilmBATask):
    """
    Download raw deletions daily. At least try, files may be missing, refs
    #12460.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("""curl --fail "{url}" > {output} """,
                          url=self.config.get('vkfilmba', 'deletions'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        """
        We expect txt, but to guarantee.
        """
        return luigi.LocalTarget(path=self.path(ext="txt"))


class VKFilmBADump(VKFilmBATask):
    """
    Download pre-built initial dump,
    https://speicherwolke.uni-leipzig.de/index.php/s/KMUldvGMJRc7iLP - XXX:
        configure this out.

    Only a single fixed item, no date.
    """

    def run(self):
        output = shellout("""curl -sL --fail "https://speicherwolke.uni-leipzig.de/index.php/s/KMUldvGMJRc7iLP/download" > {output}""")
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))



class VKFilmBAUpdates(VKFilmBATask):
    """
    Iterate over download directory itself and concat items and deletions.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'dump': VKFilmBADump(),
            'data': VKFilmBADownload(date=self.date),
            'deletions': VKFilmBADownloadDeletions(date=self.date),
        }

    def run(self):
        """
        Iterate over all zipfiles in reverse, convert and concat binary marc
        into tempfile, then deduplicate.
        """

        # Load all deletions into set.
        deleted = set()

        deldir = os.path.dirname(self.input().get('deletions').path)
        for path in sorted(iterfiles(deldir), reverse=True):
            with open(path) as handle:
                for i, line in enumerate(handle, start=1):
                    line = line.strip()
                    if len(line) > 20:
                        self.logger.warn("suspicious id: %s", line)
                    deleted.add(line)

        # Load updates.
        pattern = re.compile(r'^date-[0-9]{4,4}-[0-9]{2,2}-[0-9]{2,2}.zip$')
        datadir = os.path.dirname(self.input().get('data').path)

        # Combine all binary MARC records in this file.
        _, combined = tempfile.mkstemp(prefix='siskin-')

        for path in sorted(iterfiles(datadir), reverse=True):
            filename = os.path.basename(path)

            if not pattern.match(filename):
                self.logger.warn("ignoring invalid filename: %s", path)
                continue
            if os.stat(path).st_size < 22:
                self.logger.warn("ignoring possibly empty zip file: %s", path)
                continue

            with zipfile.ZipFile(path) as zf:
                for name in zf.namelist():
                    with zf.open(name) as handle:
                        with tempfile.NamedTemporaryFile(delete=False) as dst:
                            shutil.copyfileobj(handle, dst)
                        shellout("yaz-marcdump -i marcxml -o marc {input} >> {output}",
                                 input=dst.name, output=combined, ignoremap={5: 'expected error from yaz'})
                        os.remove(dst.name)

        # Finally, concatenate initial dump.
        shellout("cat {input} >> {output}", input=self.input().get('dump').path, output=combined)

        # Already seen identifier.
        seen = set()

        with self.output().open('wb') as output:
            writer = pymarc.MARCWriter(output)

            # Iterate over MARC records (which are newest to oldest, keep track of seen identifiers).
            with open(combined) as handle:
                reader = pymarc.MARCReader(handle, force_utf8=True, to_unicode=True)
                for record in reader:
                    field = record["001"]
                    if not field:
                        self.logger.debug("missing identifier")
                        continue

                    id = field.value()

                    if id in seen:
                        self.logger.debug("skipping duplicate: %s", id)
                        continue
                    if id in deleted:
                        self.logger.debug("skipping deleted: %s", id)
                        continue

                    self.logger.debug("adding %s", id)
                    writer.write(record)
                    seen.add(id)

        self.logger.debug("found %s unique records (deletion list contained %s ids)", len(seen), len(deleted))
        os.remove(combined)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="mrc"))


class VKFilmBAMARC(VKFilmBATask):
    """
    Run conversion script.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return VKFilmBAUpdates(date=self.date)

    def run(self):
        output = shellout("python {script} {input} {output}",
                          script=self.assets("148/148_marcbinary.py"),
                          input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='fincmarc.mrc'))
