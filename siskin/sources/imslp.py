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

"""
IMSLP, refs #1240, #13055.

Config
------

[imslp]

listings-url = http://example.com/export/
legacy-mapping = /path/to/json.file
"""

import datetime
import json
import os
import shutil
import tempfile

import luigi

from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.decorator import deprecated
from siskin.task import DefaultTask
from siskin.utils import scrape_html_listing
from siskin.conversions import imslp_tarball_to_marc

class IMSLPTask(DefaultTask):
    """ Base task for IMSLP. """
    TAG = "15"

    def closest(self):
        """ XXX: adjust. """
        return datetime.date(2017, 12, 25)

    def latest_link(self):
        """
        Find the lastest link on the listing.
        """
        listings_url = self.config.get("imslp", "listings-url")
        links = sorted([link for link in scrape_html_listing(listings_url)
                        if "imslpOut_" in link])

        self.logger.debug("found %s links on IMSLP download site", len(links))

        if len(links) == 0:
            raise ValueError("could not find any suitable links: %s", listings_url)

        return links[-1]

class IMSLPDownloadDeprecated(IMSLPTask):
    """ Download raw data. Should be a single URL pointing to a tar.gz. """

    date = ClosestDateParameter(default=datetime.date.today())

    @deprecated
    def run(self):
        output = shellout("""wget -O {output} {url}""",
                          url=self.config.get('imslp', 'url'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='tar.gz'))

class IMSLPDownload(IMSLPTask):
    """
    Download raw data. Actually keeps the original filename. Should be a tar.gz.
    """

    date = luigi.DateParameter(default=datetime.date.today())

    def run(self):
        """
        Assume, links are sortable, the last item should contain the latest link.
        XXX: Do not download, if not newer than last downloaded file.
        """
        most_recent = self.latest_link()
        self.logger.debug("most recent package seem to be: %s", most_recent)
        output = shellout(""" wget -O {output} "{url}" """, url=most_recent)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        filename = os.path.basename(self.latest_link())
        dst = os.path.join(self.taskdir(), filename)
        return luigi.LocalTarget(path=dst)

class IMSLPLegacyMapping(IMSLPTask, luigi.ExternalTask):
    """

    Path to JSON file mapping record id to viaf id and worktitle ("Werktitel").

    This is a fixed file.

        {
            "fantasiano10mudarraalonso": {
                "title": "Fantasie No.10",
                "viaf": "(VIAF)100203803"
            },
            ...
        }
    """

    def output(self):
        return luigi.LocalTarget(path=self.config.get('imslp', 'legacy-mapping'))

class IMSLPConvert(IMSLPTask):
    """
    Take a current version of the data plus legacy mapping and convert.

    WIP, refs #12288, refs #13055.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'legacy-mapping': IMSLPLegacyMapping(),
            'data': IMSLPDownload(date=self.date),
        }

    def run(self):
        """
        Load mapping, convert, write.
        """
        with self.input().get("legacy-mapping").open() as handle:
            mapping = json.load(handle)

        output = imslp_tarball_to_marc(self.input().get("data").path,
                                       legacy_mapping=mapping)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        filename = os.path.basename(self.latest_link())
        dst = os.path.join(self.taskdir(), filename.replace("tar.gz", "fincmarc.mrc"))
        return luigi.LocalTarget(path=dst)

class IMSLPConvertDeprecated(IMSLPTask):
    """
    Extract and transform.

    TODO, refs #13055 -- see IMSLPDownloadNext and IMSLPConvertNext and IMSLPLegacyMapping.

    File "/usr/lib/python2.7/site-packages/siskin/assets/15/15_marcbinary.py", line 165, in <module>
        record = record["document"]
    KeyError: 'document'
    """

    date = ClosestDateParameter(default=datetime.date.today())
    debug = luigi.BoolParameter(description='do not delete temporary folder')

    def requires(self):
        return IMSLPDownloadDeprecated(date=self.date)

    @deprecated
    def run(self):
        tempdir = tempfile.mkdtemp(prefix='siskin-')
        shellout("tar -xzf {archive} -C {tempdir}",
                 archive=self.input().path, tempdir=tempdir)
        output = shellout("python {script} {tempdir} {output}",
                          script=self.assets('15/15_marcbinary.py'), tempdir=tempdir)
        if not self.debug:
            shutil.rmtree(tempdir)
        else:
            self.logger.debug("not deleting temporary folder at %s", tempdir)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='fincmarc.mrc'))
