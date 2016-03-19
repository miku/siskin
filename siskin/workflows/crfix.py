# coding: utf-8

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

"""
Possible mitigation of https://github.com/CrossRef/rest-api-doc/issues/67.
"""

from gluish.format import TSV
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
from siskin.workflows.ai import AIIntermediateSchema
import datetime
import json
import luigi

class CRFixTask(DefaultTask):
    TAG = 'crfix'

    def closest(self):
        return monthly(date=self.date)

class CRFixTitles(CRFixTask):
    """
    Extract all article titles from intermediate schema.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return AIIntermediateSchema(date=self.date)

    def run(self):
        output = shellout("""jq -r '.["rft.atitle"]' <(unpigz -c {input}) > {output}""", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

class CRFixAuthors(CRFixTask):
    """
    Extract author names.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return AIIntermediateSchema(date=self.date)

    def run(self):
        with self.input().open() as handle:
            with self.output().open('w') as output:
                for line in handle:
                    c = json.loads(line)
                    authors = c.get("authors")
                    if authors:
                        for author in authors:
                            if author.get("rft.au"):
                                output.write_tsv(author["rft.au"].encode('utf-8'))
                                continue
                            if author.get("rft.aulast") and author.get("rft.aufirst"):
                                name = author["rft.aufirst"].encode('utf-8') + " " + author["rft.aulast"].encode('utf-8')
                                output.write_tsv(name)
                                continue
                            if author.get("rft.aulast"):
                                output.write_tsv(author["rft.aulast"].encode('utf-8'))
                                continue
                            if author.get("rft.aufirst"):
                                output.write_tsv(author["rft.aufirst"].encode('utf-8'))
                                continue

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
