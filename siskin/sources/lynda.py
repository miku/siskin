# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301

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
Lynda, refs #11477.

* https://www.lynda.com/courselist?library=en&retired=true
* https://www.lynda.com/courselist?library=de&retired=true

"""

from siskin.task import DefaultTask
from siskin.decorator import deprecated

from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
import luigi
import csv
from gluish.utils import shellout
import datetime
import json


class LyndaTask(DefaultTask):
    """
    Base class for lynda.com courses.
    """
    TAG = '141'

    def closest(self):
        return monthly(date=self.date)


class LyndaDownload(LyndaTask):
    """
    Download list.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    language = luigi.Parameter(default="en",
                               description="available languages: de, en")

    def run(self):
        url = "https://www.lynda.com/courselist?library=%s&retired=true" % self.language
        output = shellout("""curl --fail "{url}" > {output}""", url=url)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='zip'))


class LyndaIntermediateSchema(LyndaTask):
    """
    Intermediate schema conversion.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    language = luigi.Parameter(default="en",
                               description="available languages: de, en")

    def requires(self):
        return LyndaDownload(language=self.language, date=self.date)

    @deprecated
    def run(self):
        """
        It's a Zip with a single file.
        """
        output = shellout("""unzip -p {input} | dos2unix > {output}""",
                          input=self.input().path)

        language_map = {'en': 'eng', 'de': 'deu'}
        collection_map = {'en': 'English', 'de': 'Deutsch'}

        with open(output) as csvfile:
            reader = csv.DictReader(csvfile)
            with self.output().open('w') as output:
                for row in reader:
                    # ['Course ID', 'Course Title', 'Author', 'Release Date',
                    # 'Level', 'Duration', 'Category', 'Categories',
                    # 'Software', 'Description', 'Course URL', 'AICC URL', 'SSO
                    # URL', 'Small Thumbnail', 'Medium Thumbnail', 'Large
                    # Thumbnail', 'Status', 'Retire Date', 'Library']

                    # 02/13/2015
                    date = datetime.datetime.strptime(
                        row['Release Date'], '%m/%d/%Y')

                    categories = set([row['Category'], row['Categories']])
                    parts = [s.strip() for s in row['Course Title'].split(':')]

                    if len(parts) == 1:
                        title, subtitle = parts[0], ''
                    if len(parts) > 1:
                        title, subtitle = parts[0], ': '.join(parts[1:])

                    doc = {
                        'finc.id': 'ai-141-%s' % row['Course ID'],
                        'finc.record_id': row['Course ID'],
                        'finc.source_id': '141',
                        'finc.format': 'ElectronicResourceRemoteAccess',
                        'finc.mega_collection': 'Lynda.com %s' % (collection_map[self.language]),
                        'rft.genre': 'document',
                        'rft.pub': 'Lynda',
                        'rft.date': date.strftime("%Y-%m-%d"),
                        'x.date': date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                        'ris.type': 'VIDEO',
                        'languages': [language_map[self.language]],
                        'x.subjects': list(categories),
                        'rft.atitle': title,
                        'rft.subtitle': subtitle,
                        'rft.abstract': row['Description'],
                        'rft.url': [row['Course URL']],
                        'rft.author': [{'rft.au': row['Author']}],
                    }
                    output.write(json.dumps(doc))
                    output.write("\n")

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))
