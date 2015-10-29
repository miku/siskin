# coding: utf-8
#
#  Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                    The Finc Authors, http://finc.info
#                    Martin Czygan, <martin.czygan@uni-leipzig.de>
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
# Microsoft Academic Graph
#
# TOS: http://research.microsoft.com/en-us/um/redmond/projects/mag/license.txt
#
# Links:
#
# * http://research.microsoft.com/en-us/projects/mag/
# * https://academicgraph.blob.core.windows.net/graph/index.html?eulaaccept=on
#

from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
import datetime
import luigi

class MAGTask(DefaultTask):
    TAG = 'mag'

    def closest(self):
        return datetime.date(2015, 8, 20)

class MAGDump(MAGTask):
    """ Dump file. """

    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("""curl https://academicgraph.blob.core.windows.net/graph-{date}/MicrosoftAcademicGraph.zip > {output}""",
                          date=self.closest())
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="zip"))

class MAGFile(MAGTask):
    """
    Download a single file. Possible files:

    Affiliations.zip
    Authors.zip
    ConferenceInstances.zip
    ConferenceSeries.zip
    FieldsOfStudy.zip
    Journals.zip
    PaperAuthorAffiliations.zip
    PaperKeywords.zip
    PaperReferences.zip
    Papers.zip
    PaperUrls.zip

    """

    date = ClosestDateParameter(default=datetime.date.today())
    file = luigi.Parameter(default='Papers.zip')

    def run(self):
        output = shellout("""curl https://academicgraph.blob.core.windows.net/graph-{date}/{file} > {output}""",
                          date=self.closest(), file=self.file)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="zip"))

class MAGFiles(MAGTask, luigi.WrapperTask):
    """ Download all files separately. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        files = [
            'Affiliations.zip',
            'Authors.zip',
            'ConferenceInstances.zip',
            'ConferenceSeries.zip',
            'FieldsOfStudy.zip',
            'Journals.zip',
            'PaperAuthorAffiliations.zip',
            'PaperKeywords.zip',
            'PaperReferences.zip',
            'Papers.zip',
            'PaperUrls.zip',
        ]
        for file in files:
            yield MAGFile(file=file)

    def output(self):
        return self.input()
