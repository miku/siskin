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
import sys

import luigi
import requests

from siskin.task import DefaultTask


class JoveTask(DefaultTask):
    TAG = "jove"


class JoveIntermediateSchema(JoveTask):
    """
    Download and convert.
    """

    def run(self):
        r = requests.get("https://www.jove.com/api/articles/v0/articlefeed.php?csv=1")
        s = io.StringIO(r.text)
        with self.output().open('w') as output:
            for doc in csv.DictReader(s):
                converted = {
                    'doi': doc['DOI'],
                    'url': [doc['DOI Link'], doc['Link']],
                    'rft.atitle': doc['Article Title'],
                    'rft.jtitle': doc['Journal'],
                    'authors': [{"rft.au": name} for name in doc.get('Authors').split(';')],
                    'finc.record_id': 'ai-jove-%s' % doc['ArticleID'],
                    'finc.source_id': 'jove',
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
