# coding: utf-8
# pylint: disable=C0301,E1101,C0103

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
import multiprocessing
import os
import re
import shutil
import string
import tempfile
import urllib

import luigi
import rdflib
import requests
import ujson as json

from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
from gluish.common import Executable
from gluish.format import TSV, Gzip
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.benchmark import timed
from siskin.database import sqlitedb
from siskin.sources.amsl import (AMSLFilterConfigFreeze, AMSLHoldingsFile,
                                 AMSLOpenAccessKBART, AMSLService, AMSLFreeContent)
from siskin.sources.arxiv import ArxivIntermediateSchema
from siskin.sources.ceeol import CeeolJournalsDumpIntermediateSchema
from siskin.sources.crossref import (CrossrefDOIList,
                                     CrossrefIntermediateSchema,
                                     CrossrefUniqISSNList)
from siskin.sources.degruyter import (DegruyterDOIList,
                                      DegruyterIntermediateSchema,
                                      DegruyterISSNList)
from siskin.sources.disson import DissonIntermediateSchema
from siskin.sources.doaj import (DOAJDOIList, DOAJIntermediateSchema,
                                 DOAJISSNList)
from siskin.sources.elsevierjournals import (ElsevierJournalsIntermediateSchema,
                                             ElsevierJournalsISSNList)
from siskin.sources.genios import (GeniosCombinedIntermediateSchema,
                                   GeniosISSNList)
from siskin.sources.ieee import IEEEDOIList, IEEEIntermediateSchema
from siskin.sources.ijoc import IJOCIntermediateSchema
from siskin.sources.jstor import (JstorDOIList, JstorIntermediateSchema,
                                  JstorISSNList)
from siskin.sources.kielfmf import KielFMFIntermediateSchema
from siskin.sources.mag import MAGReferenceDB
from siskin.sources.pqdt import PQDTIntermediateSchema
from siskin.sources.springer import SpringerIntermediateSchema
from siskin.sources.ssoar import SSOARIntermediateSchema
from siskin.sources.thieme import ThiemeIntermediateSchema, ThiemeISSNList
from siskin.task import DefaultTask
from siskin.utils import URLCache, load_set_from_target


class AITask(DefaultTask):
    """ AI base task. """
    TAG = 'ai'

    def closest(self):
        return weekly(self.date)


class AIDOIList(AITask):
    """
    List of DOI for ai.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return [
            CrossrefDOIList(date=self.date),
            DegruyterDOIList(date=self.date),
            DOAJDOIList(date=self.date),
            ElsevierJournalsISSNList(date=self.date),
            IEEEDOIList(date=self.date),
            JstorDOIList(date=self.date),
        ]

    def run(self):
        """
        Concatenate files and run sort -u.
        """
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}",
                     input=target.path, output=stopover)
        output = shellout(
            "LC_ALL=C sort -S35% {input} > {output}", input=stopover)
        os.remove(stopover)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class AIDOIRedirectTable(AITask):
    """
    Generate a redirect table. Takes days. Make sure doi.org is in your hosts file so DNS is not stressed.
    """
    hurrly_workers = luigi.IntParameter(
        default=4, description='number of workers for hurrly')

    def requires(self):
        return AIDOIList()

    def run(self):
        output = shellout("hurrly -w {w} < {input} > {output}",
                          w=self.hurrly_workers, input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


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
        with self.output().open('w') as output:
            for k1, k2 in itertools.combinations(list(self.input().keys()), 2):
                s1 = load_set_from_target(self.input().get(k1))
                s2 = load_set_from_target(self.input().get(k2))
                output.write_tsv(k1, k2, len(s1), len(s2),
                                 len(s1.intersection(s2)))

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
        with self.output().open('w') as output:
            for k1, k2 in itertools.combinations(list(self.input().keys()), 2):
                s1 = load_set_from_target(self.input().get(k1))
                s2 = load_set_from_target(self.input().get(k2))
                output.write_tsv(k1, k2, len(s1), len(s2),
                                 len(s1.intersection(s2)))

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
        with self.output().open('w') as output:
            for k1, k2 in itertools.combinations(list(self.input().keys()), 2):
                s1 = load_set_from_target(self.input().get(k1))
                s2 = load_set_from_target(self.input().get(k2))
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
        for _, target in list(self.input().items()):
            shellout("cat {input} | grep -v null >> {output}",
                     input=target.path, output=output)
        output = shellout("sort -u {input} > {output}", input=output)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class AIQuality(AITask):
    """
    Create short quality report.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return [
            ArxivIntermediateSchema(date=self.date),
            CrossrefIntermediateSchema(date=self.date),
            DegruyterIntermediateSchema(date=self.date),
            DOAJIntermediateSchema(date=self.date),
            ElsevierJournalsIntermediateSchema(date=self.date),
            GeniosCombinedIntermediateSchema(date=self.date),
            IEEEIntermediateSchema(date=self.date),
            JstorIntermediateSchema(date=self.date),
            ThiemeIntermediateSchema(date=self.date),
        ]

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout(""" echo "S:{input}" >> {output} """,
                     input=target.path, output=stopover)
            shellout(
                "span-check <(unpigz -c {input}) 2>&1 | jq . >> {output}", input=target.path, output=stopover)
        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='txt'))


class AIIntermediateSchema(AITask):
    """ Create an intermediate schema record from all AI sources. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return [
            ArxivIntermediateSchema(date=self.date, stamp=True),
            CrossrefIntermediateSchema(date=self.date, stamp=True),
            DegruyterIntermediateSchema(date=self.date, stamp=True),
            DissonIntermediateSchema(date=self.date, stamp=True),
            DOAJIntermediateSchema(date=self.date, stamp=True),
            ElsevierJournalsIntermediateSchema(date=self.date, stamp=True),
            GeniosCombinedIntermediateSchema(date=self.date, stamp=True),
            IEEEIntermediateSchema(date=self.date, stamp=True),
            JstorIntermediateSchema(date=self.date, stamp=True),
            ThiemeIntermediateSchema(date=self.date, stamp=True),
            SpringerIntermediateSchema(stamp=True),
            PQDTIntermediateSchema(date=self.date, stamp=True),
            KielFMFIntermediateSchema(stamp=True),
            IJOCIntermediateSchema(stamp=True),
            CeeolJournalsDumpIntermediateSchema(stamp=True),
            SSOARIntermediateSchema(stamp=True),
        ]

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}",
                     input=target.path, output=stopover)
        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)


class AIAddReferences(AITask):
    """
    Experimental enrichment with linked documents.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'ai': AIIntermediateSchema(date=self.date),
            'db': MAGReferenceDB(date=self.date),
        }

    @timed
    def run(self):
        """
        Run query for DOI for each document and add a new keys if we find referenced DOI.
        """
        with sqlitedb(self.input().get('db').path) as cursor:
            with self.input().get('ai').open() as handle:
                with self.output().open('w') as output:
                    stmt = """
                    select doi from lookup where id IN (
                        select ref from refs where id = (select id from lookup where doi = ? ))
                    """
                    for line in handle:
                        doc = json.loads(line)
                        doi = doc.get('doi')
                        if doi is not None:
                            cursor.execute(stmt, (doi,))
                            rows = cursor.fetchall()
                            doc["refs"] = [v[0] for v in rows]
                        output.write(json.dumps(doc))
                        output.write("\n")

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)


class AICheckStats(AITask):
    """
    Run quality check and gather error records.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return AIIntermediateSchema(date=self.date)

    @timed
    def run(self):
        output = shellout(
            "span-check -verbose <(unpigz -c {input}) | pigz -c > {output}", input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)


class AIErrorDistribution(AITask):
    """
    Error distribution.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return AICheckStats(date=self.date)

    @timed
    def run(self):
        output = shellout(
            "unpigz -c {input} | jq -rc .err | sort | uniq -c | sort -nr > {output}", input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='txt'))


class AIRedact(AITask):
    """
    Redact intermediate schema.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return AIApplyOpenAccessFlag(date=self.date)

    @timed
    def run(self):
        """ A bit slower: `jq 'del(.["x.fulltext"])' input > output` """
        output = shellout(
            "span-redact <(unpigz -c {input}) | pigz -c > {output}", input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)


class AIBlobDB(AITask):
    """
    Create data.db file for microblob, then:

    $ microblob -db $(taskoutput AIBlobDB) -file $(taskoutput AIRedact) -serve
    """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return AIRedact(date=self.date)

    @timed
    def run(self):
        """
        Extract intermediate schema file temporarily.
        """
        tempdir = tempfile.mkdtemp(prefix="siskin-")
        extracted = shellout(
            "unpigz -c {input} > {output}", input=self.input().path)

        shellout("microblob -db {tempdir} -file {input} -key finc.id",
                 tempdir=tempdir, input=extracted)
        os.remove(extracted)
        os.makedirs(os.path.dirname(self.output().path))
        shutil.move(tempdir, self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldb'))


class AILicensing(AITask):
    """
    Take intermediate schema and a config and attach ISILs accordingly. As per
    MDM-2017-11-29 a fixed date should be used. We fix date to YYYY-MM-15, refs
    #11821, #12021. AMSLFilterConfigFreeze should be run via a daily cron.

    Example crontab (assuming virtual environment named siskin):

        00 12  * * * source $HOME/.virtualenvs/siskin/bin/activate && taskdo AMSLFilterConfigFreeze --local-scheduler
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        jourfixe = datetime.date(self.date.year, self.date.month, 15)

        if self.date.day < 15:
            jourfixe = jourfixe + relativedelta(months=-1)

        self.logger.debug("AILicensing jour-fixe at %s", jourfixe)

        return {
            'is': AIApplyOpenAccessFlag(date=self.date),
            'config': AMSLFilterConfigFreeze(date=jourfixe),
        }

    def run(self):
        """
        span v0.1.204 or later.
        """
        output = shellout("span-tag -unfreeze {config} <(unpigz -c {input}) | pigz -c > {output}",
                          config=self.input().get('config').path, input=self.input().get('is').path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)


class AILocalData(AITask):
    """
    Extract a CSV about source, id, doi and institutions for deduplication.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    batchsize = luigi.IntParameter(default=25000, significant=False)

    def requires(self):
        return AILicensing(date=self.date)

    def run(self):
        """
        Unzip on the fly, extract fields as CSV, sort be third column.
        """
        output = shellout("""unpigz -c {input} | span-local-data -b {size} | LC_ALL=C sort -S20% -t, -k3 > {output} """,
                          size=self.batchsize, input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='csv'))


class AIInstitutionChanges(AITask):
    """
    Calculate institution changes based on DOI duplicates. Experimental, using
    https://github.com/miku/groupcover with preferences.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return AILocalData(date=self.date)

    def run(self):
        output = shellout(
            """groupcover -lower -prefs '85 55 89 60 50 105 34 101 53 49 28 48 121' < {input} > {output}""", input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class AIIntermediateSchemaDeduplicated(AITask):
    """
    A DOI deduplicated version of the intermediate schema. Experimental.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'changes': AIInstitutionChanges(date=self.date),
            'file': AILicensing(date=self.date),
        }

    def run(self):
        """
        Update intermediate schema labels from file. We cut out the ID and the
        ISIL list from the changes.
        """
        output = shellout("unpigz -c {input} | span-update-labels -b 20000 -f <(cut -d, -f1,4- {file}) | pigz -c > {output}",
                          input=self.input().get('file').path,
                          file=self.input().get('changes').path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)


class AIExport(AITask):
    """
    Export to various formats

    XXX: #11467, crossref vs JSTOR.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='solr5vu3', description='export format')

    def requires(self):
        return AIIntermediateSchemaDeduplicated(date=self.date)

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


class AIUpdate(AITask, luigi.WrapperTask):
    """
    Just a wrapper task for updates, refs #5702.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return [AIExport(date=self.date), AIRedact(date=self.date)]

    def output(self):
        return self.input()


class AICollectionsAndSerialNumbers(AITask):
    """
    Turtlized coverage report, ..., refs #5156.

    XXX: The collection names are not that uniform. Would need to extract those
    names from AMSL - however, the raw data contains these names ...
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return AIIntermediateSchema(date=self.date)

    def run(self):
        """
        Create a graph with two namespaces.
        """
        g = rdflib.Graph()
        ns = {
            "amsl": rdflib.Namespace('http://amsl.technology/'),
            "disco": rdflib.Namespace('http://amsl.technology/discovery/Metadatenkollektion/'),
        }

        p = ns["amsl"].coveredMediumID
        disco = ns["disco"]

        with self.input().open() as handle:
            for i, line in enumerate(handle):
                if i % 100000 == 0:
                    self.logger.debug("%s %s", i, len(g))

                doc = json.loads(line)
                issns = list(itertools.chain(
                    doc.get('rft.issn', []), doc.get('rft.eissn', [])))
                colls = doc.get('finc.mega_collection', [])

                for issn in issns:
                    for c in colls:
                        s = disco[urllib.quote(c.encode('utf-8'))]

                        uri = 'urn:ISSN:%s' % urllib.quote(issn)
                        o = rdflib.URIRef(uri.strip())

                        g.add((s, p, o))

        with self.output().open('w') as output:
            output.write(g.serialize(format='turtle'))

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ttl'))


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
            'gbi': GeniosISSNList(date=self.date),
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

        sources = ['crossref', 'jstor', 'degruyter',
                   'doaj', 'gbi', 'elsevierjournals', 'thieme']

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

        cache = URLCache(directory=os.path.join(
            tempfile.gettempdir(), '.urlcache'))
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
                            output.write_tsv(
                                row.issn, 'ERR_NOT_IN_CATALOG', link)
                        else:
                            soup = BeautifulSoup(body)
                            rs = soup.findAll("div", {"class": "floatleft"})
                            if len(rs) == 0:
                                output.write_tsv(row.issn, 'ERR_LAYOUT', link)
                                continue
                            first = rs[0]
                            match = re.search(
                                r'Treffer([0-9]+)-([0-9]+)von([0-9]+)', first.text)
                            if match:
                                total = match.group(3)
                                output.write_tsv(
                                    row.issn, 'FOUND_RESULTS_%s' % total, link)
                            else:
                                output.write_tsv(
                                    row.issn, 'ERR_NO_MATCH', link)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class AIISSNCoverageSolrMatches(AITask):
    date = ClosestDateParameter(default=datetime.date.today())
    isil = luigi.Parameter(default='DE-15')

    def requires(self):
        return AICoverageISSN(date=self.date, isil=self.isil)

    def run(self):
        if self.isil != 'DE-15':
            raise RuntimeError('not implemented except for DE-15')

        cache = URLCache(directory=os.path.join(
            tempfile.gettempdir(), '.urlcache'))
        adapter = requests.adapters.HTTPAdapter(max_retries=3)
        cache.sess.mount('http://', adapter)

        finc = self.config.get('ai', 'finc-solr')
        ai = self.config.get('ai', 'ai-solr')

        def numFound(link):
            """ Given a SOLR query URL, return the number of docs found. """
            body = cache.get(link)
            content = json.loads(body)
            return content['response']['numFound']

        with self.input().open() as handle:
            with self.output().open('w') as output:
                for i, row in enumerate(handle.iter_tsv(cols=('issn', 'status'))):
                    if row.status == 'NOT_FOUND':
                        link = '%s/select?q=institution:%s+AND+issn:%s&wt=json' % (
                            finc, self.isil, row.issn)
                        self.logger.info('fetch #%05d: %s', i, link)
                        output.write_tsv('finc', row.issn,
                                         numFound(link), link)
                    else:
                        link = '%s/select?q=institution:%s+AND+issn:%s&wt=json' % (
                            ai, self.isil, row.issn)
                        self.logger.info('fetch #%05d: %s', i, link)
                        output.write_tsv('ai', row.issn, numFound(link), link)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class AIApplyOpenAccessFlag(AITask):
    """
    Apply OA-Flag. Experimental, refs. #8986.

    1. Inhouse/Flag (per collection)
    2. KBART
    3. Gold-OA
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'amslfc': AMSLFreeContent(date=self.date),
            'kbart': AMSLOpenAccessKBART(date=self.date),
            'file': AIIntermediateSchema(date=self.date),
        }

    def run(self):
        """
        Python: 500k recs/min, Go (span-oa-filter): 2.5M recs/min, refs #11285#note-17, refs #11969.

        XXX: Testing: Adjust filtered file with data from AMSLFreeContent.

        Exclude 48 explicitlty (span 0.1.225 and later) with -xsid, refs #12738.
        """

        output = shellout("""unpigz -c {input} |
                             span-oa-filter -xsid 48 -f {kbart} -fc {amslfc} |
                             pigz -c > {output}""",
                          input=self.input().get('file').path,
                          kbart=self.input().get('kbart').path,
                          amslfc=self.input().get('amslfc').path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)
