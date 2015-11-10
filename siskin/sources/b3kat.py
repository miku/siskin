# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301
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
B3Kat open data. http://www.b3kat.de/
"""

from gluish.common import Executable
from gluish.format import TSV
from gluish.intervals import quarterly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.benchmark import timed
from siskin.task import DefaultTask
from siskin.utils import random_string
import datetime
import luigi
import random
import requests
import tempfile
import time
import urllib
import urlparse

class B3KatTask(DefaultTask):
    """ Base task for http://www.b3kat.de/ related things. """
    TAG = 'b3kat'

    def closest(self):
        return quarterly()

class B3KatDownload(B3KatTask):
    """
    Download dump, extract and concat on the fly.
    Note: Output will be about 80G in size.

    Download URL is hardcoded and points to:

    http://lod.b3kat.de/download/lod.b3kat.de.part{part}.ttl.gz
    """
    date = ClosestDateParameter(default=datetime.date.today())
    template = luigi.Parameter(default='http://lod.b3kat.de/download/lod.b3kat.de.part{part}.ttl.gz',
                               significant=False)

    def requires(self):
        return Executable(name='curl')

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for i in range(27):
            url = self.template.format(part=i)
            shellout("""curl --retry 1 --compress "{url}" >> {output}""",
                     url=url, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class B3KatSameAs(B3KatTask):
    """ Extract the sameAs information in an ugly way. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'content': B3KatDownload(date=self.date),
                'apps': Executable(name='serdi', message='http://drobilla.net/software/serd/')}

    def run(self):
        """ TODO: try https://github.com/miku/nttoldj """
        output = shellout("""serdi -b -f -i turtle -o ntriples {input} | LANG=C grep "owl#sameAs" > {output}""",
                          input=self.input().get('content').path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='n3'))

class B3KatExtractRelation(B3KatTask):
    """
    Extract MARC relations.
    List of relators: http://www.loc.gov/marc/relators/relaterm.html
    """
    date = ClosestDateParameter(default=datetime.date.today())
    relator = luigi.Parameter(default="http://id.loc.gov/vocabulary/relators/aut",
                              description='http://www.loc.gov/marc/relators/relaterm.html')

    def requires(self):
        return {'content': B3KatDownload(date=self.date),
                'apps': Executable(name='serdi', message='http://drobilla.net/software/serd/')}

    def run(self):

        output = shellout("""serdi -b -f -i turtle -o ntriples {input} | LANG=C grep "{relator}" > {output}""",
                          relator=self.relator, input=self.input().get('content').path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='n3', digest=True))

class B3KatShadowHarvest(B3KatTask):
    """
    Shadow harvest bug. Investigate bug, where subsequent GET requests
    to the same URL return different results.
    """

    tag = luigi.Parameter(default=random_string(), description='run on every call')

    # request parameters
    scheme = luigi.Parameter(default='http', significant=False)
    netloc = luigi.Parameter(default='bvbr.bib-bvb.de:8991', significant=False)
    urlpath = luigi.Parameter(default='F', significant=False)
    func = luigi.Parameter(default='service', significant=False)
    doc_library = luigi.Parameter(default='BVB01', significant=False)
    doc_number = luigi.Parameter(default='020028928', significant=False)
    service_type = luigi.Parameter(default='MEDIA', significant=False)

    loop = luigi.IntParameter(default=100, description='repeat request this many times',
                              significant=False)

    def run(self):
        query = urllib.urlencode(dict(func=self.func, doc_library=self.doc_library,
                                      doc_number=self.doc_number, service_type=self.service_type))
        url = urlparse.urlunparse((self.scheme, self.netloc, self.urlpath, '', query, ''))
        with self.output().open('w') as output:
            for _ in range(self.loop):
                try:
                    r = requests.get(url, timeout=20)
                    output.write(r.text)
                    output.write("\n\n\n\n")
                    time.sleep(random.random())
                except Exception as err:
                    self.logger.error(err)
                    break

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
