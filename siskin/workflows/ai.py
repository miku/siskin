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

#
# Aggregated Index workflows
# ==========================
#
# AMSL (api) + AIIntermediateSchema (sources) = AILicensing (isil) -> AIExport (for solr)
#

from BeautifulSoup import BeautifulSoup
from gluish.common import Executable
from gluish.format import TSV, Gzip
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.benchmark import timed
from siskin.configuration import Config
from siskin.sources.amsl import AMSLHoldingsFile, AMSLCollections, AMSLHoldingsISILList, AMSLCollectionsISIL
from siskin.sources.crossref import CrossrefIntermediateSchema, CrossrefUniqISSNList
from siskin.sources.degruyter import DegruyterIntermediateSchema, DegruyterISSNList
from siskin.sources.doaj import DOAJIntermediateSchema, DOAJISSNList
from siskin.sources.gbi import GBIIntermediateSchemaByKind
from siskin.sources.gbi import GBIISSNList
from siskin.sources.holdings import HoldingsFile
from siskin.sources.jstor import JstorIntermediateSchema, JstorISSNList
from siskin.task import DefaultTask
from siskin.utils import URLCache
import collections
import datetime
import itertools
import json
import luigi
import os
import re
import requests
import string
import tempfile

config = Config.instance()

class AITask(DefaultTask):
    TAG = 'ai'

    def closest(self):
        return weekly(self.date)

class DownloadFile(AITask):
    """ Download a file. TODO(miku): move this out here. """

    date = luigi.DateParameter(default=datetime.date.today())
    url = luigi.Parameter()

    @timed
    def run(self):
        output = shellout("""curl -L --fail "{url}" > {output}""", url=self.url)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='file', digest=True))

class DownloadAndUnzipFile(AITask):
    """ Download a file and unzip it. TODO(miku): move this out here. """
    date = luigi.DateParameter(default=datetime.date.today())
    url = luigi.Parameter()

    @timed
    def run(self):
        output = shellout("""curl -L --fail "{url}" > {output}""", url=self.url)
        output = shellout("""unzip -qq -p {input} > {output}""", input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='file', digest=True))

class AIISSNStats(AITask):
    """ Match ISSN lists. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'crossref': CrossrefUniqISSNList(date=self.date),
            'degruyter': DegruyterISSNList(date=self.date),
            'doaj': DOAJISSNList(date=self.date),
            'jstor': JstorISSNList(date=self.date)
        }

    @timed
    def run(self):
        def loadset(target):
            s = set()
            with target.open() as handle:
                for row in handle.iter_tsv(cols=('issn',)):
                    s.add(row.issn)
            return s

        with self.output().open('w') as output:
            for k1, k2 in itertools.combinations(self.input().keys(), 2):
                s1 = loadset(self.input().get(k1))
                s2 = loadset(self.input().get(k2))
                output.write_tsv(k1, k2, len(s1), len(s2), len(s1.intersection(s2)))

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class AIISSNOverlaps(AITask):
    """ Match ISSN lists. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'crossref': CrossrefUniqISSNList(date=self.date),
            'degruyter': DegruyterISSNList(date=self.date),
            'doaj': DOAJISSNList(date=self.date),
            # 'gbi': GBIISSNList(date=self.date),
            'jstor': JstorISSNList(date=self.date)
        }

    @timed
    def run(self):
        def loadset(target):
            s = set()
            with target.open() as handle:
                for row in handle.iter_tsv(cols=('issn',)):
                    s.add(row.issn)
            return s

        with self.output().open('w') as output:
            for k1, k2 in itertools.combinations(self.input().keys(), 2):
                s1 = loadset(self.input().get(k1))
                s2 = loadset(self.input().get(k2))
                for issn in sorted(s1.intersection(s2)):
                    output.write_tsv(k1, k2, issn)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class AIIntermediateSchema(AITask):
    """ Create an intermediate schema record from all AI sources. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        """
        TODO(miku): Dynamic dependencies based on holding file.
        """
        return [Executable(name='pigz', message='http://zlib.net/pigz/'),
                GBIIntermediateSchemaByKind(kind='references', date=self.date),
                GBIIntermediateSchemaByKind(kind='fulltext', date=self.date),
                CrossrefIntermediateSchema(date=self.date),
                DegruyterIntermediateSchema(date=self.date),
                DOAJIntermediateSchema(date=self.date),
                JstorIntermediateSchema(date=self.date)]

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for target in self.input()[1:]:
            shellout("cat {input} >> {output}", input=target.path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)

class AIFilterConfigISIL(AITask):
    """
    Next iteration of filtering, based on various AMSL responses.
    Creates the filters for a single ISIL from AMSL.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    isil = luigi.Parameter(description='ISIL')

    def requires(self):
        return AMSLCollectionsISIL(isil=self.isil)

    @timed
    def run(self):
        filters = []
        with self.input().open() as handle:
            c = json.load(handle)

        for source, colls in c.iteritems():
            filters.append({'and': [{'source': source}, {'collections': colls}]})

        with self.output().open('w') as output:
            output.write(json.dumps({self.isil: {'or': filters}}))

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='json'))

class AIFilterConfigNext(AITask):
    """
    Next iteration of filtering, based on various AMSL responses.
    ISILs are hard-coded, but should come from the API as well.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'DE-15': AIFilterConfigISIL(isil='DE-15'),
            'DE-14': AIFilterConfigISIL(isil='DE-14'),
        }

    @timed
    def run(self):
        config = {}
        for isil, target in self.input().iteritems():
            with target.open() as handle:
                c = json.load(handle)
                config[isil] = c[isil]

        with self.output().open('w') as output:
            output.write(json.dumps(config))

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='json'))

class AIFilterConfig(AITask):
    """
    Create a filter configuration from AMSL.

    The filterconfig dictionary should be built from AMSL data only: holdings
    and collections, and information about which source or collection uses
    which method of labeling.

    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        """
        Intermediate schema files and holdings.
        """
        return {
            # holding files
            'DE-105': AMSLHoldingsFile(isil='DE-105'),
            'DE-14': AMSLHoldingsFile(isil='DE-14'),
            'DE-15': AMSLHoldingsFile(isil='DE-15'),
            'DE-1972': AMSLHoldingsFile(isil='DE-1972'),
            'DE-8': AMSLHoldingsFile(isil='DE-8'),
            'DE-Bn3': AMSLHoldingsFile(isil='DE-Bn3'),
            'DE-Brt1': AMSLHoldingsFile(isil='DE-Brt1'),
            'DE-Ch1': AMSLHoldingsFile(isil='DE-Ch1'),
            'DE-D117': AMSLHoldingsFile(isil='DE-D117'),
            'DE-D161': AMSLHoldingsFile(isil='DE-D161'),
            'DE-Gla1': AMSLHoldingsFile(isil='DE-Gla1'),
            'DE-J59': AMSLHoldingsFile(isil='DE-J59'),
            'DE-Ki95': AMSLHoldingsFile(isil='DE-Ki95'),
            'DE-Rs1': AMSLHoldingsFile(isil='DE-Rs1'),
            'DE-Zi4': AMSLHoldingsFile(isil='DE-Zi4'),
            'DE-Kn38': AMSLHoldingsFile(isil='DE-Kn38'),

            # issn list
            'DE-15-FID': DownloadFile(date=self.date, url='https://goo.gl/tm6U9D'),
        }

    @timed
    def run(self):
        """
        Build a filter from a template. Fill in the missing files.
        """
        with open(self.assets('filterconf.template.json')) as handle:
            template = json.load(handle)

        config = {}

        for isil, filtertree in template.iteritems():
            template = json.dumps(filtertree)
            if "{{ file }}" in template and isil not in self.input():
                raise RuntimeError('isil %s seems to expect input file, but none given as dependency' % isil)
            tree = template.replace("{{ file }}", self.input().get(isil).path)
            config[isil] = json.loads(tree)

        with self.output().open('w') as output:
            output.write(json.dumps(config))

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='json'))

class AILicensing(AITask):
    """
    Take intermediate schema and a config and attach ISILs accordingly.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'is': AIIntermediateSchema(date=self.date),
            'config': AIFilterConfig(date=self.date),
        }

    def run(self):
        output = shellout("span-tag -c {config} <(unpigz -c {input}) | pigz -c > {output}",
                          config=self.input().get('config').path, input=self.input().get('is').path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))

class AIDuplicates(AITask):
    """
    First attempt at a list of id, DOI, ISIL list for simple duplicate detection.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return AILicensing(date=self.date)

    def run(self):
        output = shellout("""jq -r '[.["finc.record_id"]?, .doi?, .["rft.atitle"]?, .["x.labels"][]?] | @csv' <(unpigz -c {input}) > {output} """,
                          input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class AIDuplicatesSortbyDOI(AITask):
    """
    Sort list by DOI.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return AIDuplicates(date=self.date)

    def run(self):
        output = shellout("""LC_ALL=C sort -t , -k2,2 -S35% {input} > {output} """, input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class AIExport(AITask):
    """
    Next iteration of AI export.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return AILicensing(date=self.date)

    def run(self):
        output = shellout("span-export <(unpigz -c {input}) | pigz -c > {output}", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))

class AIUpdate(AITask, luigi.WrapperTask):
    """
    Just a wrapper task for updates, refs #5702.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return [AIExport(date=self.date), AIIntermediateSchema(date=self.date)]

    def output(self):
        return self.input()

class AICoverageISSN(AITask):
    """
    For all AI sources and all holding files, find the number
    (perc) of ISSNs in the holding file, that we have some reach to.

    Result looks something like this, indicating availablity and sources:

    2091-2145   crossref|doaj
    2091-2234   crossref
    2091-2560   crossref
    2091-2609   crossref
    2091-2730   NOT_FOUND

    """
    date = ClosestDateParameter(default=datetime.date.today())
    isil = luigi.Parameter(default='DE-15')

    def requires(self):
        """
        TODO: Add GBI ISSN list.
        """
        return {
            'crossref': CrossrefUniqISSNList(date=self.date),
            'jstor': JstorISSNList(date=self.date),
            'degruyter': DegruyterISSNList(date=self.date),
            'doaj': DOAJISSNList(date=self.date),
            'gbi': GBIISSNList(date=self.date),
            'file': AMSLHoldingsFile(isil=self.isil),
        }

    def run(self):
        issns = collections.defaultdict(set)

        with self.input().get('file').open() as handle:
            for row in handle:
                fields = row.strip().split('\t')
                if len(fields) < 3:
                    continue

                if re.search(r'[0-9]{4}-[0-9]{3}[0-9X]', fields[1]):
                    issns['file'].add(fields[1])

                if re.search(r'[0-9]{4}-[0-9]{3}[0-9X]', fields[2]):
                    issns['file'].add(fields[2])

        sources = ['crossref', 'jstor', 'degruyter', 'doaj', 'gbi']

        for source in sources:
            with self.input().get(source).open() as handle:
                for row in handle:
                    issns[source].add(row.strip())

        with self.output().open('w') as output:
            for issn in sorted(issns['file']):
                fields = []
                for source in sources:
                    if issn in issns[source]:
                        fields.append(source)
                if len(fields) == 0:
                    output.write_tsv(issn, "NOT_FOUND")
                else:
                    output.write_tsv(issn, "|".join(fields))

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class AIISSNCoverageCatalogMatches(AITask):
    """
    For all ISSN, that are NOT_FOUND in AI, do a HTTP request to the current
    catalog and scrape the number of results.

    Example output:

    0171-4880   FOUND_RESULTS_1 https://katalog.ub.uni-...
    0171-5801   ERR_NOT_IN_CATALOG  https://katalog.ub....
    0171-5860   FOUND_RESULTS_466   https://katalog.ub....
    0171-7227   FOUND_RESULTS_2 https://katalog.ub.uni-...

    """
    date = ClosestDateParameter(default=datetime.date.today())
    isil = luigi.Parameter(default='DE-15')

    def requires(self):
        return AICoverageISSN(date=self.date, isil=self.isil)

    def run(self):
        if not self.isil == 'DE-15':
            raise RuntimeError('not implemented except for DE-15')

        cache = URLCache(directory=os.path.join(tempfile.gettempdir(), '.urlcache'))
        adapter = requests.adapters.HTTPAdapter(max_retries=3)
        cache.sess.mount('http://', adapter)

        with self.input().open() as handle:
            with self.output().open('w') as output:
                for i, row in enumerate(handle.iter_tsv(cols=('issn', 'status'))):
                    if row.status == 'NOT_FOUND':
                        link = 'https://katalog.ub.uni-leipzig.de/Search/Results?lookfor=%s&type=ISN' % row.issn
                        self.logger.info('fetch #%05d: %s' % (i, link))
                        body = cache.get(link)
                        if 'Keine Ergebnisse!' in body:
                            output.write_tsv(row.issn, 'ERR_NOT_IN_CATALOG', link)
                        else:
                            soup = BeautifulSoup(body)
                            rs = soup.findAll("div", {"class" : "floatleft"})
                            if len(rs) == 0:
                                output.write_tsv(row.issn, 'ERR_LAYOUT', link)
                                continue
                            first = rs[0]
                            match = re.search(r'Treffer([0-9]+)-([0-9]+)von([0-9]+)', first.text)
                            if match:
                                total = match.group(3)
                                output.write_tsv(row.issn, 'FOUND_RESULTS_%s' % total, link)
                            else:
                                output.write_tsv(row.issn, 'ERR_NO_MATCH', link)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class AIISSNCoverageSolrMatches(AITask):
    date = ClosestDateParameter(default=datetime.date.today())
    isil = luigi.Parameter(default='DE-15')

    def requires(self):
        return AICoverageISSN(date=self.date, isil=self.isil)

    def run(self):
        if not self.isil == 'DE-15':
            raise RuntimeError('not implemented except for DE-15')

        cache = URLCache(directory=os.path.join(tempfile.gettempdir(), '.urlcache'))
        adapter = requests.adapters.HTTPAdapter(max_retries=3)
        cache.sess.mount('http://', adapter)

        finc = config.get('ai', 'finc-solr')
        ai = config.get('ai', 'ai-solr')

        with self.input().open() as handle:
            with self.output().open('w') as output:
                for i, row in enumerate(handle.iter_tsv(cols=('issn', 'status'))):
                    if row.status == 'NOT_FOUND':
                        link = '%s/select?q=institution:%s+AND+issn:%s&wt=json' % (finc, self.isil, row.issn)
                        self.logger.info('fetch #%05d: %s' % (i, link))
                        body = cache.get(link)
                        content = json.loads(body)
                        output.write_tsv('finc', row.issn, content['response']['numFound'], link)
                    else:
                        link = '%s/select?q=institution:%s+AND+issn:%s&wt=json' % (ai, self.isil, row.issn)
                        self.logger.info('fetch #%05d: %s' % (i, link))
                        body = cache.get(link)
                        content = json.loads(body)
                        output.write_tsv('ai', row.issn, content['response']['numFound'], link)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class AIISSNDetailedCoverageReport(AITask):
    """
    Detailed coverage report with Pandas (local dependency).
    """
    date = ClosestDateParameter(default=datetime.date.today())
    isil = luigi.Parameter(default='DE-15')

    def requires(self):
        return AIISSNCoverageSolrMatches(date=self.date, isil=self.isil)

    def run(self):
        import pandas as pd

        metrics = collections.defaultdict(dict)
        df = pd.read_csv(self.input().path, sep='\t', names=['shard', 'issn', 'count'], usecols=[0, 1, 2])

        # number of articles per shard
        metrics['sum']['finc'] = sum(df[df.shard == 'finc']['count'])
        metrics['sum']['ai'] = sum(df[df.shard == 'ai']['count'])

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class AIISSNCoverageReport(AITask):
    """
    For all ISILs, see what percentage of ISSN are covered in AI.

    Example report:

    DE-105  52.65
    DE-14   53.06
    DE-15   53.41
    DE-1972 43.99
    DE-8    52.94
    DE-Bn3  50.53
    DE-Brt1 43.99
    DE-Ch1  52.35
    DE-D117 43.99
    DE-D161 51.88
    DE-Gla1 51.88
    DE-Ki95 54.08
    DE-L229 50.83
    DE-Pl11 43.99
    DE-Rs1  51.11
    DE-Zi4  52.45

    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        prerequisite = AMSLHoldingsISILList(date=self.date)
        luigi.build([prerequisite])
        isils = set()
        with prerequisite.output().open() as handle:
            for isil in (string.strip(line) for line in handle):
                isils.add(isil)

        return dict((isil, AICoverageISSN(date=self.date, isil=isil)) for isil in isils)

    def run(self):
        with self.output().open('w') as output:
            for isil, target in sorted(self.input().iteritems()):
                counter = collections.Counter()
                with target.open() as handle:
                    for line in handle:
                        if 'NOT_FOUND' in line:
                            counter['miss'] += 1
                        else:
                            counter['hits'] += 1
                total = counter['hits'] + counter['miss']
                if total == 0:
                    continue
                ratio = (100.0 / total) * counter['hits']
                output.write_tsv(isil, '%0.2f' % ratio)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
