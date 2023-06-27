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

import binascii
import collections
import datetime
import itertools
import json
import multiprocessing
import os
import re
import shutil
import string
import subprocess
import tempfile
import urllib

import luigi
import rdflib
import requests
import six
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
from gluish.common import Executable
from gluish.format import TSV, Zstd
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.benchmark import timed
from siskin.database import sqlitedb
from siskin.sources.amsl import (AMSLFilterConfigFreeze, AMSLFreeContent, AMSLHoldingsFile, AMSLOpenAccessKBART, AMSLService)
from siskin.sources.base import BaseFix
from siskin.sources.ceeol import CeeolIntermediateSchema
from siskin.sources.crossref import (CrossrefDOIList, CrossrefFeedFile, CrossrefIntermediateSchema, CrossrefUniqISSNList)
from siskin.sources.degruyter import (DegruyterDOIList, DegruyterIntermediateSchema, DegruyterISSNList)
from siskin.sources.doaj import (DOAJDOIList, DOAJIntermediateSchema, DOAJISSNList)
from siskin.sources.elsevierjournals import ElsevierJournalsISSNList
from siskin.sources.genderopen import GenderopenIntermediateSchema
from siskin.sources.jstor import (JstorDOIList, JstorIntermediateSchemaCombined, JstorISSNList)
from siskin.sources.kalliope import KalliopeDirectDownload
from siskin.sources.lissa import LissaIntermediateSchema
from siskin.sources.olc import OLCIntermediateSchema
from siskin.sources.osf import OSFIntermediateSchema
from siskin.sources.thieme import ThiemeIntermediateSchema, ThiemeISSNList
from siskin.task import DefaultTask
from siskin.utils import URLCache, load_set_from_target


class AITask(DefaultTask):
    """AI base task."""

    TAG = "ai"

    def closest(self):
        return weekly(self.date)


# Tasks for sigelage and deduplication, solr export and blob server export follow.
#
# AIIntermediateSchema (merge)
# AIRedact (prepare file for blob)
# AILicensing (apply licensing)
#
# AILocalData                      |
# AIInstitutionChanges             | (doi deduplication per isil)
# AIIntermediateSchemaDeduplicated |

# AIExport (solr schema)
#
# See also: https://git.io/JUTOO


class AIIntermediateSchema(AITask):
    """
    Create an intermediate schema record from all AI sources. Everything that
    is put here, will pass along the ISIL attachment and deduplication
    pipeline.

    All inputs must be zstd compressed.
    """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return [
            CeeolIntermediateSchema(stamp=True),
            CrossrefIntermediateSchema(date=self.date, stamp=True),
            DOAJIntermediateSchema(date=self.date, stamp=True, format="doaj-oai"),
            DegruyterIntermediateSchema(date=self.date, stamp=True),
            GenderopenIntermediateSchema(date=self.date, stamp=True),
            JstorIntermediateSchemaCombined(date=self.date, stamp=True),
            LissaIntermediateSchema(date=self.date, stamp=True),
            OLCIntermediateSchema(date=self.date, stamp=True),
            OSFIntermediateSchema(date=self.date, stamp=True),
            ThiemeIntermediateSchema(date=self.date, stamp=True),
        ]

    @timed
    def run(self):
        """
        Check, if all files are zstd compressed before we concatenate.

        https://datatracker.ietf.org/doc/html/rfc8478#section-3.1.1
        """
        for target in self.input():
            with open(target.path, "rb") as f:
                head = f.read(4)
                if binascii.hexlify(head) not in (b"fd2fb528", b"28b52ffd"):
                    raise RuntimeError("AIIntermediateSchema requires zstd-compressed inputs, failed: %s (got: %s)" % (target.path, binascii.hexlify(head)))

        _, stopover = tempfile.mkstemp(prefix="siskin-")
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path, output=stopover)
        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="ldj.zst"), format=Zstd)


class AIRedact(AITask):
    """
    Redact intermediate schema.
    """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return AIApplyOpenAccessFlag(date=self.date)

    @timed
    def run(self):
        """
        A bit slower: jq 'del(.["x.fulltext"])' input > output
        """
        output = shellout(
            "span-redact <(zstd -cd -T0 {input}) | zstd -c -T0 > {output}",
            input=self.input().path,
        )
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="ldj.zst"), format=Zstd)


class AILicensing(AITask):
    """
    Take intermediate schema and a config and attach ISILs accordingly. As per
    MDM-2017-11-29 a fixed date should be used. We fix date to YYYY-MM-15, refs
    #11821, #12021. AMSLFilterConfigFreeze should be run via a daily cron.

    Example crontab (assuming virtual environment named siskin):

        00 12  * * * source $HOME/.virtualenvs/siskin/bin/activate && taskdo AMSLFilterConfigFreeze --local-scheduler
    """

    date = ClosestDateParameter(default=datetime.date.today())
    override = luigi.BoolParameter(description="do not use jour fixe", significant=False)
    drop = luigi.BoolParameter(description="drop records w/o isil")
    style = luigi.Parameter(default="default", description="licensing style, e.g. default or reduced")

    def requires(self):
        if self.override:
            jourfixe = self.date
        else:
            jourfixe = datetime.date(self.date.year, self.date.month, 15)
            if self.date.day < 15:
                jourfixe = jourfixe + relativedelta(months=-1)

        self.logger.debug("AILicensing date: %s (override=%s)", jourfixe, self.override)

        return {
            "is": AIApplyOpenAccessFlag(date=self.date),
            "config": AMSLFilterConfigFreeze(date=jourfixe, style=self.style),
        }

    def run(self):
        """
        A recent version of span might be required.
        """
        if self.drop:
            output = shellout(
                "span-tag -D -unfreeze {config} <(zstd -cd -T0 {input}) | zstd -c -T0 > {output}",
                config=self.input().get("config").path,
                input=self.input().get("is").path,
            )
        else:
            output = shellout(
                "span-tag -unfreeze {config} <(zstd -cd -T0 {input}) | zstd -c -T0 > {output}",
                config=self.input().get("config").path,
                input=self.input().get("is").path,
            )
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="ldj.zst"), format=Zstd)


class AILocalData(AITask):
    """
    Extract a CSV about source, id, doi and institutions for deduplication.
    """

    date = ClosestDateParameter(default=datetime.date.today())
    batchsize = luigi.IntParameter(default=25000, significant=False)
    style = luigi.Parameter(default="default", description="licensing style, e.g. default or reduced")

    def requires(self):
        return AILicensing(date=self.date, drop=True, style=self.style)

    def run(self):
        """
        Unzip on the fly, extract fields as CSV, sort be third column.
        """
        output = shellout(
            """
            zstd -cd -T0 {input} |
            span-local-data -b {size} |
            LC_ALL=C sort --ignore-case -S20% -t, -k3 > {output}
            """,
            size=self.batchsize,
            input=self.input().path,
        )
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="csv"))


class AIInstitutionChanges(AITask):
    """
    Calculate institution changes based on DOI duplicates. Experimental, using
    https://github.com/miku/groupcover with preferences.
    """

    date = ClosestDateParameter(default=datetime.date.today())
    style = luigi.Parameter(default="default", description="licensing style, e.g. default or reduced")

    def requires(self):
        return AILocalData(date=self.date, style=self.style)

    def run(self):
        output = shellout(
            """
            groupcover -lower -prefs '85 55 89 60 50 105 101 53 49 28 48 121' < {input} > {output}
            """,
            input=self.input().path,
        )
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class AIIntermediateSchemaDeduplicated(AITask):
    """
    A DOI deduplicated version of the intermediate schema. Experimental.
    """

    date = ClosestDateParameter(default=datetime.date.today())
    style = luigi.Parameter(default="default", description="licensing style, e.g. default or reduced")

    def requires(self):
        return {
            "changes": AIInstitutionChanges(date=self.date, style=self.style),
            "file": AILicensing(date=self.date, drop=True, style=self.style),
        }

    def run(self):
        """
        Update intermediate schema labels from file. We cut out the ID and the
        ISIL list from the changes.
        """
        output = shellout(
            """
            zstd -cd -T0 {input} |
            span-update-labels -b 20000 -f <(cut -d, -f1,4- {file}) | zstd -c -T0 > {output}
            """,
            input=self.input().get("file").path,
            file=self.input().get("changes").path,
        )
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="ldj.zst"), format=Zstd)


class AIExport(AITask):
    """
    Export to various formats. Include SOLR-ready (isil'd) BASE file as well.

    XXX: #11467, crossref vs JSTOR.
    """

    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default="solr5vu3", description="export format")
    style = luigi.Parameter(default="default", description="licensing style, e.g. default or reduced")

    def requires(self):
        return {
            "ai": AIIntermediateSchemaDeduplicated(date=self.date, style=self.style),
            "base": BaseFix(style="z"),
            # "kalliope": KalliopeDirectDownload(), # (was) refs. https://projekte.ub.uni-leipzig.de/issues/22596#note-10
        }

    def run(self):
        _, tmp = tempfile.mkstemp(prefix="siskin-")
        # already converted data
        filenames = [
            self.input().get("base").path,
            # self.input().get("kalliope").path,
        ]
        for fn in filenames:
            shellout(
                """
                cat "{fn}" >> "{output}"
                """,
                fn=fn,
                output=tmp,
            )
        # turn intermediate schema into solr
        shellout(
            """
            span-export -o solr5vu3 <(zstd -cd -T0 {input}) | zstd -c -T0 >> {output}
            """,
            format=self.format,
            input=self.input().get("ai").path,
            output=tmp,
        )
        luigi.LocalTarget(tmp).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="zst"), format=Zstd)


class AIUpdate(AITask, luigi.WrapperTask):
    """
    A wrapper task for updates, refs #5702.
    """

    date = ClosestDateParameter(default=datetime.date.today())
    style = luigi.Parameter(default="default", description="licensing style, e.g. default or reduced")

    def requires(self):
        return [AIExport(date=self.date, style=self.style), AIRedact(date=self.date)]

    def output(self):
        return self.input()


class AIPartialUpdate(AITask):
    """
    Prepare a partial, indexable file (from crossref).
    """
    date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(days=2))

    def requires(self):
        return {
            "amsl": AMSLFilterConfigFreeze(date=self.date),
            "crossref-feed-file": CrossrefFeedFile(date=self.date),
        }

    def run(self):
        output = shellout("""
                 zstdcat -T0 {input} |
                 span-import -i crossref |
                 span-tag -unfreeze {amsl} |
                 span-export |
                 zstd -c -T0 > {output}""",
                 input=self.input().get("crossref-feed-file").path,
                 amsl=self.input().get("amsl").path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="zst"), format=Zstd)

class AIPartialUpdatePublish(AITask):
    """
    Publish latest crossref snapshot to currently live index.
    """
    date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(days=2))

    def requires(self):
        return AIPartialUpdate(date=self.date)

    def run(self):
        started = datetime.datetime.now()
        solr = subprocess.getoutput("echo $(siskin-whatislive.sh solr_live)/solr/biblio")
        if len(solr) < 15:
            raise RuntimeError("unexpected solr url: {}".format(solr))
        shellout("""
                 echo "solrbulk {input} => {solr}"
                 """, input=self.input().path, solr=solr)
        # shellout("""
        #          zstdcat -T0 {input} | solrbulk -server {solr} -commit 5000000
        #          """, input=self.input().path, solr=solr)
        stopped = datetime.datetime.now()
        elapsed = stopped - started
        with self.output().open("wb") as output:
            json.dump({
                "started": started.isoformat(),
                "stopped": stopped.isoformat(),
                "elapsed_s": elapsed.seconds,
                "solr": solr,
                "file": self.input().path,
            }, output)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="json"))

# Other tasks
# -----------


class AIDOIStats(AITask):
    """
    DOI overlaps between various sources.
    """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            "crossref": CrossrefDOIList(date=self.date),
            "degruyter": DegruyterDOIList(date=self.date),
            "doaj": DOAJDOIList(date=self.date),
            "jstor": JstorDOIList(date=self.date),
        }

    @timed
    def run(self):
        with self.output().open("w") as output:
            for k1, k2 in itertools.combinations(list(self.input().keys()), 2):
                s1 = load_set_from_target(self.input().get(k1))
                s2 = load_set_from_target(self.input().get(k2))
                output.write_tsv(k1, k2, str(len(s1)), str(len(s2)), str(len(s1.intersection(s2))))

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class AIISSNStats(AITask):
    """
    Match ISSN lists. Note: This should be OLAP, not batch.
    """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            "crossref": CrossrefUniqISSNList(date=self.date),
            "degruyter": DegruyterISSNList(date=self.date),
            "doaj": DOAJISSNList(date=self.date),
            "jstor": JstorISSNList(date=self.date),
        }

    @timed
    def run(self):
        with self.output().open("w") as output:
            for k1, k2 in itertools.combinations(list(self.input().keys()), 2):
                s1 = load_set_from_target(self.input().get(k1))
                s2 = load_set_from_target(self.input().get(k2))
                output.write_tsv(k1, k2, len(s1), len(s2), len(s1.intersection(s2)))

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class AIISSNOverlaps(AITask):
    """
    Match ISSN lists. Note: This should be OLAP, not batch.
    """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            "crossref": CrossrefUniqISSNList(date=self.date),
            "degruyter": DegruyterISSNList(date=self.date),
            "doaj": DOAJISSNList(date=self.date),
            "jstor": JstorISSNList(date=self.date),
        }

    @timed
    def run(self):
        with self.output().open("w") as output:
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
            "crossref": CrossrefUniqISSNList(date=self.date),
            "degruyter": DegruyterISSNList(date=self.date),
            "doaj": DOAJISSNList(date=self.date),
            "jstor": JstorISSNList(date=self.date),
            "thieme": ThiemeISSNList(date=self.date),
            "elsevier": ElsevierJournalsISSNList(date=self.date),
        }

    @timed
    def run(self):
        _, output = tempfile.mkstemp(prefix="siskin-")
        for _, target in list(self.input().items()):
            shellout(
                "cat {input} | grep -v null >> {output}",
                input=target.path,
                output=output,
            )
        output = shellout("sort -u {input} > {output}", input=output)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


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
            "amsl": rdflib.Namespace("http://amsl.technology/"),
            "disco": rdflib.Namespace("http://amsl.technology/discovery/Metadatenkollektion/"),
        }

        p = ns["amsl"].coveredMediumID
        disco = ns["disco"]

        with self.input().open() as handle:
            for i, line in enumerate(handle):
                if i % 100000 == 0:
                    self.logger.debug("%s %s", i, len(g))

                doc = json.loads(line)
                issns = list(itertools.chain(doc.get("rft.issn", []), doc.get("rft.eissn", [])))
                colls = doc.get("finc.mega_collection", [])

                for issn in issns:
                    for c in colls:
                        s = disco[urllib.quote(c.encode("utf-8"))]

                        uri = "urn:ISSN:%s" % urllib.quote(issn)
                        o = rdflib.URIRef(uri.strip())

                        g.add((s, p, o))

        with self.output().open("w") as output:
            output.write(g.serialize(format="turtle"))

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="ttl"))


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
    isil = luigi.Parameter(default="DE-15")

    def requires(self):
        """
        TODO: Add GBI ISSN list.
        """
        return {
            "crossref": CrossrefUniqISSNList(date=self.date),
            "jstor": JstorISSNList(date=self.date),
            "degruyter": DegruyterISSNList(date=self.date),
            "doaj": DOAJISSNList(date=self.date),
            "elsevierjournals": ElsevierJournalsISSNList(date=self.date),
            "thieme": ThiemeISSNList(date=self.date),
            "file": AMSLHoldingsFile(isil=self.isil),
        }

    def run(self):
        issns = collections.defaultdict(set)

        with self.input().get("file").open() as handle:
            for row in handle:
                fields = row.strip().split("\t")
                if len(fields) < 3:
                    continue

                if re.search(r"[0-9]{4}-[0-9]{3}[0-9X]", fields[1]):
                    issns["file"].add(fields[1])

                if re.search(r"[0-9]{4}-[0-9]{3}[0-9X]", fields[2]):
                    issns["file"].add(fields[2])

        sources = [
            "crossref",
            "jstor",
            "degruyter",
            "doaj",
            "gbi",
            "elsevierjournals",
            "thieme",
        ]

        for source in sources:
            with self.input().get(source).open() as handle:
                for row in handle:
                    issns[source].add(row.strip())

        with self.output().open("w") as output:
            for issn in sorted(issns["file"]):
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
    isil = luigi.Parameter(default="DE-15")

    def requires(self):
        return AICoverageISSN(date=self.date, isil=self.isil)

    def run(self):
        if not self.isil == "DE-15":
            raise RuntimeError("not implemented except for DE-15")

        cache = URLCache(directory=os.path.join(tempfile.gettempdir(), ".urlcache"))
        adapter = requests.adapters.HTTPAdapter(max_retries=3)
        cache.sess.mount("http://", adapter)

        with self.input().open() as handle:
            with self.output().open("w") as output:
                for i, row in enumerate(handle.iter_tsv(cols=("issn", "status"))):
                    if row.status == "NOT_FOUND":
                        link = ("https://katalog.ub.uni-leipzig.de/Search/Results?lookfor=%s&type=ISN" % row.issn)
                        self.logger.info("fetch #%05d: %s" % (i, link))
                        body = cache.get(link)
                        if "Keine Ergebnisse!" in body:
                            output.write_tsv(row.issn, "ERR_NOT_IN_CATALOG", link)
                        else:
                            soup = BeautifulSoup(body)
                            rs = soup.findAll("div", {"class": "floatleft"})
                            if len(rs) == 0:
                                output.write_tsv(row.issn, "ERR_LAYOUT", link)
                                continue
                            first = rs[0]
                            match = re.search(r"Treffer([0-9]+)-([0-9]+)von([0-9]+)", first.text)
                            if match:
                                total = match.group(3)
                                output.write_tsv(row.issn, "FOUND_RESULTS_%s" % total, link)
                            else:
                                output.write_tsv(row.issn, "ERR_NO_MATCH", link)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class AIISSNCoverageSolrMatches(AITask):
    date = ClosestDateParameter(default=datetime.date.today())
    isil = luigi.Parameter(default="DE-15")

    def requires(self):
        return AICoverageISSN(date=self.date, isil=self.isil)

    def run(self):
        if self.isil != "DE-15":
            raise RuntimeError("not implemented except for DE-15")

        cache = URLCache(directory=os.path.join(tempfile.gettempdir(), ".urlcache"))
        adapter = requests.adapters.HTTPAdapter(max_retries=3)
        cache.sess.mount("http://", adapter)

        finc = self.config.get("ai", "finc-solr")
        ai = self.config.get("ai", "ai-solr")

        def numFound(link):
            """Given a SOLR query URL, return the number of docs found."""
            body = cache.get(link)
            content = json.loads(body)
            return content["response"]["numFound"]

        with self.input().open() as handle:
            with self.output().open("w") as output:
                for i, row in enumerate(handle.iter_tsv(cols=("issn", "status"))):
                    if row.status == "NOT_FOUND":
                        link = "%s/select?q=institution:%s+AND+issn:%s&wt=json" % (
                            finc,
                            self.isil,
                            row.issn,
                        )
                        self.logger.info("fetch #%05d: %s", i, link)
                        output.write_tsv("finc", row.issn, numFound(link), link)
                    else:
                        link = "%s/select?q=institution:%s+AND+issn:%s&wt=json" % (
                            ai,
                            self.isil,
                            row.issn,
                        )
                        self.logger.info("fetch #%05d: %s", i, link)
                        output.write_tsv("ai", row.issn, numFound(link), link)

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
            "amslfc": AMSLFreeContent(date=self.date),
            "kbart": AMSLOpenAccessKBART(date=self.date),
            "file": AIIntermediateSchema(date=self.date),
        }

    def run(self):
        """
        Python: 500k recs/min, Go (span-oa-filter): 2.5M recs/min, refs #11285#note-17, refs #11969.

        XXX: Testing: Adjust filtered file with data from AMSLFreeContent.

        Exclude 48 explicitlty (span 0.1.225 and later) with -xsid, refs
        #12738. Mark sids as open access via -oasid (span 0.1.272 and later).
        """

        output = shellout(
            """
            zstd -cd -T0 {input} |
            span-oa-filter -b 25000 -f {kbart} -fc {amslfc} -xsid 48 -oasid 28 -oasid 30 -oasid 34 |
            zstd -c -T0 > {output}""",
            input=self.input().get("file").path,
            kbart=self.input().get("kbart").path,
            amslfc=self.input().get("amslfc").path,
        )
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="ldj.zst"), format=Zstd)


class AIDOIList(AITask):
    """
    List of DOI for a given ISIL,

    taskcat AILicensing | jq -r 'select(.["x.labels"][]? | contains ("DE-15")) | .doi?'
    """

    date = ClosestDateParameter(default=datetime.date.today())
    isil = luigi.Parameter(default="DE-15")

    def requires(self):
        return AILicensing(date=self.date)

    def run(self):
        output = shellout(
            """
            zstd -cd -T0 {input} |
            jq -r 'select(.["x.labels"][]? | contains ("{isil}")) | .doi?' > {output}
            """,
            input=self.input().path,
            isil=self.isil,
        )
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="tsv"))
