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
Electronic Resource Management System Based on Linked Data Technologies.

http://amsl.technology

Config:

[amsl]

endpoint = http://111.11.111.111:8890/sparql
isil-rel = https://hello.example/static/about.json

"""

from siskin.configuration import Config
from siskin.task import DefaultTask
import datetime
import json
import luigi
import requests
import urllib

config = Config.instance()

class AMSLTask(DefaultTask):
    TAG = 'amsl'

class AMSLISILForSourceID(AMSLTask):
    """
    Query amsl to find ISILs for source ID. This will dump the raw query
    results.
    """

    date = luigi.DateParameter(default=datetime.date.today())
    source_id = luigi.Parameter(default='0')
    format = luigi.Parameter(default='json')
    url = luigi.Parameter(default=config.get('amsl', 'endpoint'),
                          description='sparql query endpoint url', significant=False)

    def run(self):
        with open(self.assets('amsl.collections.sparql')) as handle:
            template = handle.read()
        qs = template.replace("SOURCE_ID", self.source_id)

        r = requests.get("%s?%s" % (self.url, urllib.urlencode({'query': qs, 'format': self.format})))
        if not r.status_code == 200:
            raise RuntimeError("%s %s" % (r.status_code, r.text))

        with self.output().open('w') as output:
            output.write(r.text.encode('utf-8').strip())

    def output(self):
       return luigi.LocalTarget(path=self.path(ext=self.format))

class AMSLCollections(AMSLTask):
    """ Get a list of ISIL, source and collection choices via JSON API. """

    date = luigi.DateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("""curl --fail "{link}" > {output} """, link=config.get('amsl', 'isil-rel'))
        luigi.File(output).move(self.output().path)

    def output(self):
       return luigi.LocalTarget(path=self.path())
