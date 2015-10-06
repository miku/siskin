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
[holdings]

sparql-endpoint = https://example.com/sparql-vt
"""

from gluish.utils import shellout
from siskin.configuration import Config
from siskin.task import DefaultTask
import datetime
import json
import luigi
import re
import requests
import urllib

config = Config.instance()

def query_for_isil(isil):
    """
    For a given ISIL, return the path to the corresponding
    holdings file in AMSL.
    """
    if not re.match('[0-9a-zA-Z-]+', isil):
        raise RuntimeError('invalid ISIL: %s' % isil)

    return """
    prefix lobid: <http://purl.org/lobid/lv#>
    prefix amsl: <http://vocab.ub.uni-leipzig.de/amsl/>
    select ?path
    from <http://amsl.technology/consortial/>
    from <http://lobid.org/>
    where {
        ?bib amsl:linkToHoldingsFile ?path .
        ?bib lobid:isil ?isil .
        FILTER(STRENDS(?isil, "%s"))
    }
    """ % (isil)

class HoldingsTask(DefaultTask):
    TAG = 'holdings'

class HoldingsFile(HoldingsTask):
    """ Make a Holdings-File available for a given ISIL. """

    date = luigi.Parameter(default=datetime.date.today())
    isil = luigi.Parameter(default='DE-15')

    def run(self):
        encoded = urllib.urlencode({'default-graph-uri': '',
            'format': 'application/json', 'query': query_for_isil(self.isil)})

        url = "%s?%s" % (config.get('holdings', 'sparql-endpoint'), encoded)
        r = requests.get(url)
        if r.status_code >= 400:
            raise RuntimeError('%s on %s' % (r.status_code, url))

        response = json.loads(r.text)
        bindings = response['results']['bindings']

        if len(bindings) == 0:
            raise RuntimeError('no holdings found for %s' % self.isil)
        if len(bindings) > 1:
            raise RuntimeError('ambiguous holdings found for %s' % self.isil)

        holdings_url = bindings[0]['path']['value']
        output = shellout("""curl --fail "{url}" > {output}""", url=holdings_url)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))

class HoldingsFinc(HoldingsTask, luigi.WrapperTask):
    """ Download finc-related holdings. """

    date = luigi.Parameter(default=datetime.date.today())

    def requires(self):
        isils = ['DE-15', 'DE-14', 'DE-105', 'DE-Ch1', 'DE-Gla1', 'DE-Bn3']
        return [HoldingsFile(isil=isil, date=self.date) for isil in isils]

    def output(self):
        return self.input()
