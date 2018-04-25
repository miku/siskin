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
IMSLP, refs #1240.

Config
------

[imslp]

url = http://example.com/imslpOut_2016-12-25.tar.gz
"""

import datetime
import shutil
import tempfile

import luigi
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.task import DefaultTask


class IMSLPTask(DefaultTask):
    """ Base task for IMSLP. """
    TAG = "15"

    def closest(self):
        return datetime.date(2017, 12, 25)


class IMSLPDownload(IMSLPTask):
    """ Download raw data. Should be a single URL pointing to a tar.gz. """

    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("""wget -O {output} {url}""",
                          url=self.config.get('imslp', 'url'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='tar.gz'))


class IMSLPDownloadNext(IMSLPTask):
    """
    Download raw data, roughly once per month. Should be a tar.gz.
    """

    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        raise NotImplementedError(
            "use pattern: imslpOut_YYYY-MM-DD.tar.gz, could be any day of the month")

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='tar.gz'))


class IMSLPLegacyMapping(IMSLPTask, luigi.ExternalTask):
    """

    Path to JSON file mapping record id to viaf id and worktitle ("Werktitel").

    This is a fixed file.

    Example:

        {
            "fantasiano10mudarraalonso": {
                "title": "Fantasie No.10",
                "viaf": "(VIAF)100203803"
            }
        }
    """

    def output(self):
        return luigi.LocalTarget(path=self.config.get('imslp', 'legacy-mapping'))


class IMSLPConvertNext(IMSLPTask):
    """
    Take a current version of the data plus legacy mapping and convert.

    WIP, refs #12288, refs #13055.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    debug = luigi.BoolParameter(description='do not delete temporary folder')

    def requires(self):
        return {
            'legacy-mapping': IMSLPLegacyMapping(),
            'data': IMSLPDownloadNext(date=self.date),
        }

    def run(self):
        """
        TODO: add --legacy-mapping flag to 15_marcbinary.py
        """
        tempdir = tempfile.mkdtemp(prefix='siskin-')
        shellout("tar -xzf {archive} -C {tempdir}",
                 archive=self.input().path, tempdir=tempdir)
        output = shellout("python {script} --legacy-mapping {mapping} {tempdir} {output}",
                          script=self.assets('15/15_marcbinary.py'), tempdir=tempdir)
        if not self.debug:
            shutil.rmtree(tempdir)
        else:
            self.logger.debug("not deleting temporary folder at %s", tempdir)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='fincmarc.mrc'))


class IMSLPConvert(IMSLPTask):
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
        return IMSLPDownload(date=self.date)

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
