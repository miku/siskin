# coding: utf-8
# pylint: disable=F0401,W0232,E1101,C0103,C0301,W0223,E1123,R0904,E1103

# Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
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
Directory of Open Access Journals.

DOAJ is an online directory that indexes and provides access to high quality,
open access, peer-reviewed journals.

http://doaj.org
"""

import datetime
import itertools
import operator
import tempfile
import time
from builtins import map, range

import luigi
import ujson as json
from gluish.common import Executable
from gluish.format import Gzip, TSV
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

import elasticsearch
from siskin.benchmark import timed
from siskin.task import DefaultTask
from siskin.utils import load_set_from_file, load_set_from_target


class DOAJTask(DefaultTask):
    """
    Base task for DOAJ.
    """
    TAG = '28'

    def closest(self):
        return monthly(date=self.date)


class DOAJCSV(DOAJTask):
    """
    CSV dump, updated every 30 minutes. Winter 2018: 12313x58.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    url = luigi.Parameter(default='http://doaj.org/csv', significant=False)

    def requires(self):
        return Executable(name='wget', message='http://www.gnu.org/software/wget/')

    @timed
    def run(self):
        output = shellout('wget --retry-connrefused {url} -O {output}',
                          url=self.url)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='csv'))


class DOAJDump(DOAJTask):
    """
    Simplify DOAJ harvest, via doajfetch (https://git.io/fQ2la), which will use
    the API
    (https://doaj.org/api/v1/docs#!/Search/get_api_v1_search_articles_search_query).

    As of Fall 2018 DOAJ works on a few infrastructure issue
    (https://blog.doaj.org/2018/10/01/infrastructure-and-why-sustainable-funding-so-important-to-services-like-doaj/).

    In the future, a full download should be used to lower the overhead of HTTP
    and API calls.

    As of Feb 2019, the API is not usable for full downloads any more.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    batch_size = luigi.IntParameter(default=100, significant=False, description="probably not more than 100 allowed")
    sleep = luigi.IntParameter(default=4, significant=False, description="sleep seconds between requests")

    def run(self):
        output = shellout("doajfetch -verbose -sleep {sleep}s -size {size} -P -o {output} > /dev/null",
                          sleep=self.sleep, size=self.batch_size)
        output = shellout("jq -rc '.results[]' < {input} > {output}", input=output)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class DOAJHarvest(DOAJTask):
    """
    Harvest via OAI endpoint.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("METHA_DIR={dir} metha-sync {endpoint} && METHA_DIR={dir} metha-cat {endpoint} | pigz -c > {output}",
                          dir=self.config.get('core', 'metha-dir'), endpoint='http://www.doaj.org/oai.article')
        self.logger.debug("compressing output (might take a few minutes) ...")
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml.gz'))

class DOAJIdentifierBlacklist(DOAJTask):
    """
    Create a blacklist of identifiers.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    min_title_length = luigi.IntParameter(default=30,
                                          description='Only consider titles with at least this length.')

    def requires(self):
        return DOAJDump(date=self.date)

    def run(self):
        """
        Output a list of identifiers of documents, which have the same title. Use the newest.
        """
        output = shellout("""jq -rc '[
                                .["bibjson"]["title"],
                                .["last_updated"],
                                .["id"]
                            ]' {input} | sort -S35% > {output}""", input=self.input().path)

        with open(output) as handle:
            docs = (json.loads(s) for s in handle)

            with self.output().open('w') as output:
                for title, grouper in itertools.groupby(docs, key=operator.itemgetter(0)):
                    group = list(grouper)
                    if not title or len(title) < self.min_title_length or len(group) < 2:
                        continue
                    for dropable in map(operator.itemgetter(2), group[0:len(group) - 1]):
                        output.write_tsv(dropable)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class DOAJFiltered(DOAJTask):
    """
    Filter DOAJ by ISSN in assets. Slow.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default="doaj", description="kind of source document, doaj or doaj-api")

    def requires(self):
        """
        Temporarily support both formats. XXX: Switch to doaj-api.
        """
        return {
            'dump': DOAJDump(date=self.date),
            'blacklist': DOAJIdentifierBlacklist(date=self.date),
        }

    @timed
    def run(self):
        identifier_blacklist = load_set_from_target(
            self.input().get('blacklist'))
        excludes = load_set_from_file(self.assets('028_doaj_filter.tsv'),
                                      func=lambda line: line.replace("-", ""))

        with self.output().open('w') as output:
            with self.input().get('dump').open() as handle:
                for line in handle:
                    record, skip = json.loads(line), False
                    if record['id'] in identifier_blacklist:
                        continue
                    for issn in record["bibjson"]["journal"]["issns"]:
                        issn = issn.replace("-", "").strip()
                        if issn in excludes:
                            skip = True
                            break
                    if skip:
                        continue
                    output.write(line)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))


class DOAJIntermediateSchema(DOAJTask):
    """
    Convert to intermediate schema via span; elasticsearch and API are
    currently defunkt, use OAI (with metha 0.1.37 or higher).

    TODO(miku): Keep DOAJIdentifierBlacklist updated via DOAJHarvest.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default="doaj-oai",
                             description="kind of source document, doaj-oai (defunkt: doaj, doaj-api)")

    def requires(self):
        return {
            'span-import': Executable(name='span-import', message='http://git.io/vI8NV'),
            'input': DOAJHarvest(date=self.date),
            'blacklist': DOAJIdentifierBlacklist(date=self.date),
        }

    @timed
    def run(self):
        output = shellout("""unpigz -c {input} |
                             span-import -i {format} |
                             grep -vf {exclude} |
                             grep -vf {blacklist} |
                             pigz -c > {output}""",
                          blacklist=self.input().get('blacklist').path,
                          exclude=self.assets('028_doaj_filter_issn.tsv'),
                          input=self.input().get('input').path,
                          format=self.format)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))


class DOAJExport(DOAJTask):
    """
    Export to various formats
    """
    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='solr5vu3', description='export format')

    def requires(self):
        return DOAJIntermediateSchema(date=self.date)

    def run(self):
        output = shellout("span-export -o {format} <(unpigz -c {input}) | pigz -c > {output}",
                          format=self.format, input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        extensions = {
            'solr5vu3': 'ldj.gz',
            'formeta': 'form.gz',
        }
        return luigi.LocalTarget(path=self.path(ext=extensions.get(self.format, 'gz')))


class DOAJISSNList(DOAJTask):
    """
    A list of DOAJ ISSNs.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'input': DOAJIntermediateSchema(date=self.date),
                'jq': Executable(name='jq', message='http://git.io/NYpfTw')}

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout("""jq -r '.["rft.issn"][]?' <(unpigz -c {input}) >> {output} """,
                 input=self.input().get('input').path, output=stopover)
        shellout("""jq -r '.["rft.eissn"][]?' <(unpigz -c {input}) >> {output} """,
                 input=self.input().get('input').path, output=stopover)
        output = shellout("""sort -u {input} > {output} """, input=stopover)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class DOAJDOIList(DOAJTask):
    """
    An best-effort list of DOAJ DOIs.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'input': DOAJIntermediateSchema(date=self.date),
            'jq': Executable(name='jq', message='http://git.io/NYpfTw'),
        }

    @timed
    def run(self):
        output = shellout("""jq -r '.doi' <(unpigz -c {input}) | grep -v "null" | grep -o "10.*" 2> /dev/null | sort -u > {output} """,
                          input=self.input().get('input').path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

