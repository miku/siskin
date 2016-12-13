# coding: utf-8
# pylint: disable=C0301

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

#
# Aggregated Index workflows
# ==========================
#
# AMSL (api) + AIIntermediateSchema (sources) = AILicensing (isil) -> AIExport (for solr)
#

import collections
import datetime
import itertools
import json
import os
import re
import string
import tempfile

import luigi
import requests
from BeautifulSoup import BeautifulSoup
from gluish.common import Executable
from gluish.format import TSV, Gzip
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.benchmark import timed
from siskin.sources.amsl import (AMSLFilterConfig, AMSLHoldingsFile,
                                 AMSLOpenAccessISSNList)
from siskin.sources.crossref import (CrossrefDOIList,
                                     CrossrefIntermediateSchema,
                                     CrossrefUniqISSNList)
from siskin.sources.degruyter import (DegruyterDOIList,
                                      DegruyterIntermediateSchema,
                                      DegruyterISSNList)
from siskin.sources.doaj import (DOAJDOIList, DOAJIntermediateSchema,
                                 DOAJISSNList)
from siskin.sources.elsevierjournals import (ElsevierJournalsIntermediateSchema,
                                             ElsevierJournalsISSNList)
from siskin.sources.gbi import GBIIntermediateSchemaByKind, GBIISSNList
from siskin.sources.ieee import IEEEIntermediateSchema
from siskin.sources.jstor import (JstorDOIList, JstorIntermediateSchema,
                                  JstorISSNList)
from siskin.sources.thieme import ThiemeIntermediateSchema, ThiemeISSNList
from siskin.task import DefaultTask
from siskin.utils import URLCache


class AITask(DefaultTask):
    TAG = 'ai'

    def closest(self):
        return weekly(self.date)

class AIDOIStats(AITask):
    """
    DOI overlaps.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'crossref': CrossrefDOIList(date=self.date),
            'degruyter': DegruyterDOIList(date=self.date),
            'doaj': DOAJDOIList(date=self.date),
            'jstor': JstorDOIList(date=self.date)
        }

    @timed
    def run(self):
        def loadset(target):
            s = set()
            with target.open() as handle:
                for line in handle:
                    s.add(line.strip())
            return s

        with self.output().open('w') as output:
            for k1, k2 in itertools.combinations(self.input().keys(), 2):
                s1 = loadset(self.input().get(k1))
                s2 = loadset(self.input().get(k2))
                output.write_tsv(k1, k2, len(s1), len(s2), len(s1.intersection(s2)))

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

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

class AIISSNList(AITask):
    """
    List of uniq ISSNs in AI.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'crossref': CrossrefUniqISSNList(date=self.date),
            'degruyter': DegruyterISSNList(date=self.date),
            'doaj': DOAJISSNList(date=self.date),
            'jstor': JstorISSNList(date=self.date),
            'thieme': ThiemeISSNList(date=self.date),
            'elsevier': ElsevierJournalsISSNList(date=self.date),
        }

    @timed
    def run(self):
        _, output = tempfile.mkstemp(prefix='siskin-')
        for _, target in self.input().items():
            shellout("cat {input} | grep -v null >> {output}", input=target.path, output=output, pipefail=True)
        output = shellout("sort -u {input} > {output}", input=output)
        luigi.LocalTarget(output).move(self.output().path)

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
                JstorIntermediateSchema(date=self.date),
                ThiemeIntermediateSchema(date=self.date),
                ElsevierJournalsIntermediateSchema(date=self.date),
                IEEEIntermediateSchema(date=self.date)]

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for target in self.input()[1:]:
            shellout("cat {input} >> {output}", input=target.path, output=stopover)
        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)

class AICheck(AITask):
    """
    Run quality check and gather error records.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return AIIntermediateSchema(date=self.date)

    @timed
    def run(self):
        output = shellout("span-check -verbose <(unpigz -c {input}) | pigz -c > {output}", input=self.input().path, pipefail=True)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)

class AICheckStats(AITask):
    """
    Error distribution.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return AICheck(date=self.date)

    @timed
    def run(self):
        output = shellout("unpigz -c {input} | jq -rc .err | sort | uniq -c | sort -nr > {output}", input=self.input().path, pipefail=True)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='txt'))

class AIRedact(AITask):
    """
    Redact intermediate schema.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return AIIntermediateSchema(date=self.date)

    @timed
    def run(self):
        """ A bit slower: `jq 'del(.["x.fulltext"])' input > output` """
        output = shellout("span-redact <(unpigz -c {input}) | pigz -c > {output}", input=self.input().path, pipefail=True)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)

class AILicensing(AITask):
    """
    Take intermediate schema and a config and attach ISILs accordingly.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'is': AIApplyOpenAccessFlag(date=self.date),
            'config': AMSLFilterConfig(date=self.date),
        }

    def run(self):
        output = shellout("span-tag -c {config} <(unpigz -c {input}) | pigz -c > {output}",
                          config=self.input().get('config').path, input=self.input().get('is').path, pipefail=True)
        luigi.LocalTarget(output).move(self.output().path)

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
        luigi.LocalTarget(output).move(self.output().path)

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
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class AIExport(AITask):
    """
    Export to various formats
    """
    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='solr5vu3', description='export format')

    def requires(self):
        return AILicensing(date=self.date)

    def run(self):
	output = shellout("span-export -o {format} <(unpigz -c {input}) | pigz -c > {output}", format=self.format, input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
	extensions = {
	    'solr5vu3': 'ldj.gz',
	    'formeta': 'form.gz',
	}
	return luigi.LocalTarget(path=self.path(ext=extensions.get(self.format, 'gz')))

class AIUpdate(AITask, luigi.WrapperTask):
    """
    Just a wrapper task for updates, refs #5702.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return [AIExport(date=self.date), AIRedact(date=self.date)]

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
            'elsevierjournals': ElsevierJournalsISSNList(date=self.date),
            'thieme': ThiemeISSNList(date=self.date),
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

        sources = ['crossref', 'jstor', 'degruyter', 'doaj', 'gbi', 'elsevierjournals', 'thieme']

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

        finc = self.config.get('ai', 'finc-solr')
        ai = self.config.get('ai', 'ai-solr')

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

class AIApplyOpenAccessFlag(AITask):
    """
    Apply OA-Flag. Experimental, refs. #8986.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'issns': AMSLOpenAccessISSNList(date=self.date),
            'file': AIIntermediateSchema(date=self.date),
        }

    def run(self):
        """
        Toy implementation, make this fast with mf (assets/issue/8986/...).

        Pure python, about 500k records/s.
        """
        issns = set()

        with self.input().get('issns').open() as handle:
            for line in map(string.strip, handle):
                if not line:
                    continue
                issns.add(line)

        with self.output().open('w') as output:
            with self.input().get('file').open() as handle:
                for line in handle:
                    doc = json.loads(line)
                    doc["x.oa"] = False

                    for issn in doc.get("rft.issn", []):
                        if issn in issns:
                            doc["x.oa"] = True

                    output.write(json.dumps(doc) + "\n")


    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)
