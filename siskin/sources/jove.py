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
JOVE refs #11308.
"""

import csv
import datetime
import io
import json
import os
import sys

import luigi
import requests
from gluish.utils import shellout

from siskin.decorator import deprecated
from siskin.task import DefaultTask


class JoveTask(DefaultTask):
    TAG = "143"


class JoveMARC(JoveTask):
    """
    Turn MRK MARC (https://www.loc.gov/marc/makrbrkr.html) into MARC XML via
    experimental marctexttoxml (https://git.io/vdCUN).
    """

    debug = luigi.BoolParameter(default=False, significant=False)
    date = luigi.DateParameter(default=datetime.date.today())

    def run(self):
        cleanup = []

        url = 'https://www.jove.com/api/articles/v0/articlefeed.php?marc=1&sections[]=0&sections[]=4'
        output = shellout(
            """curl -sLg '{url}' | head -n -1 > {output}""", url=url)
        cleanup.append(output)

        output = shellout(
            "marctexttoxml < {input} | xmllint --format - > {output}", input=output)
        luigi.LocalTarget(output).move(self.output().path)

        for item in cleanup:
            try:
                os.remove(item)
            except OSError:
                self.logger.warn('could not delete temporary files')

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))


class JoveFincMARC(JoveTask):
    """
    Convert JOVE to fincmarc.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return JoveMARC(date=self.date)

    def run(self):
        output = shellout("""python {script} {input} {output}""",
                          script=self.assets("143/143_marcbinary.py"),
                          input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='fincmarc.mrc'))


class JoveIntermediateSchemaFromCSV(JoveTask):
    """
    Download and convert. Just a prototype working with the CSV output.
    """

    @deprecated
    def run(self):
        r = requests.get(
            "https://www.jove.com/api/articles/v0/articlefeed.php?csv=1")
        s = io.StringIO(r.text)
        with self.output().open('w') as output:
            for doc in csv.DictReader(s):
                converted = {
                    'doi': doc['DOI'],
                    'url': [doc['DOI Link'], doc['Link']],
                    'rft.atitle': doc['Article Title'],
                    'rft.jtitle': doc['Journal'],
                    'authors': [{"rft.au": name} for name in doc.get('Authors').split(';')],
                    'finc.record_id': doc['ArticleID'],
                    'finc.source_id': 'jove',
                    'finc.id': 'ai-jove-%s' % doc['ArticleID'],
                    'x.subjects': doc['Section'].split(','),
                    'rft.date': datetime.datetime.strptime(doc['Date Published'], '%m/%d/%Y').strftime("%Y-%m-%d"),
                    'rft.genre': 'article',
                    'version': '0.9',
                    'ris.type': 'EJOUR',
                    'finc.mega_collection': 'JoVE',
                    'abstract': doc.get('Summary'),
                }
                output.write(json.dumps(converted))
                output.write("\n")

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))
