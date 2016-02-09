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
#
# Aggregated Index workflows
# ==========================
#
# TODO:
#
# Split licensing and export format. Split span-export into:
#
# 1. Intermediate Schema (IS) + AMSL Holdings API -> Licenced IS
# 2. Licenced IS + AMSL Collections API -> Filtered Licenced IS
# 3. Filtered Licenced IS -> SOLR export
#
# Examples:
#
# $ islabel -file DE-15:path/to/holdings.kbart -issnlist DE-X:path/issn.list is.0.ldj > is.1.ldj
# $ isfilter -collections DE-15:path/to/allowed.collections.txt is.1.ldj > is.2.ldj
# $ span-export -o solr-flavor-19 is.2.ldj > solr.ldj
#
# Or something along those lines.
#

from siskin.benchmark import timed
from gluish.common import Executable
from gluish.format import TSV
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.sources.crossref import CrossrefIntermediateSchema, CrossrefUniqISSNList
from siskin.sources.degruyter import DegruyterIntermediateSchema, DegruyterISSNList
from siskin.sources.doaj import DOAJIntermediateSchema, DOAJISSNList
from siskin.sources.doi import DOIBlacklist
from siskin.sources.gbi import GBIIntermediateSchemaByKind
from siskin.sources.holdings import HoldingsFile
from siskin.sources.jstor import JstorIntermediateSchema, JstorISSNList
from siskin.task import DefaultTask
import collections
import datetime
import itertools
import luigi
import tempfile

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
        return [Executable(name='pigz', message='http://zlib.net/pigz/'),
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
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))

class AILicensing(AITask):
    """
    Label intermediate schema records with ISIL. Coverage is given by holding files.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        """
        Intermediate schema files and holdings.
        """
        return {
            'crossref': CrossrefIntermediateSchema(date=self.date),
            'degruyter': DegruyterIntermediateSchema(date=self.date),
            'doaj': DOAJIntermediateSchema(date=self.date),
            'jstor': JstorIntermediateSchema(date=self.date),

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

            # 'DE-15-FID': DownloadFile(...),
        }

    @timed
    def run(self):
        for k, v in self.input().iteritems():
            pass

        # $ islabel -kbart DE-15:{x} -kbart DE-14:{x} is.ldj > is.lic.ldj

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))

class AICollectionsFilter(AITask):
    """
    For each record, see if an ISIL actually is interested in the collection.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'is': AILicensing(date=self.date),
            # 'collections': AMSLCollectionAPIResponse(...)
        }

    @timed
    def run(self):
        """
        TODO: Filter here by comparing collection names.
        """
        pass

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))

class AIExport(AITask):
    """ Create a SOLR-importable file. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'crossref': CrossrefIntermediateSchema(date=self.date),
            'degruyter': DegruyterIntermediateSchema(date=self.date),
            'doaj': DOAJIntermediateSchema(date=self.date),
            'jstor' : JstorIntermediateSchema(date=self.date),

            'DE-105': DownloadAndUnzipFile(date=self.date, url='https://goo.gl/Gq199T'),
            'DE-14': DownloadAndUnzipFile(date=self.date, url='https://goo.gl/Tz3vbk'),
            'DE-15': DownloadAndUnzipFile(date=self.date, url='https://goo.gl/inyKLr'),
            'DE-Bn3': DownloadAndUnzipFile(date=self.date, url='https://goo.gl/oq8LDD'),
            'DE-Ch1': DownloadAndUnzipFile(date=self.date, url='https://goo.gl/uJwoUf'),
            'DE-Gla1': DownloadAndUnzipFile(date=self.date, url='https://goo.gl/6506Dz'),
            'DE-Zi4': DownloadAndUnzipFile(date=self.date, url='https://goo.gl/Ld0LCw'),
            'DE-J59': DownloadAndUnzipFile(date=self.date, url='https://goo.gl/44xEbF'),

            'DE-15-FID': DownloadFile(date=self.date, url='https://goo.gl/tm6U9D'),

            'blacklist': DOIBlacklist(date=self.date),
            'app': Executable(name='span-export', message='http://git.io/vI8NV'),
            'pigz': Executable(name='pigz', message='http://zlib.net/pigz/'),
        }

    @timed
    def run(self):
        """
        TODO(miku): Move source/ISIL matrix and filter method out of here.
        """
        _, stopover = tempfile.mkstemp(prefix='siskin-')

        # DE-15 and DE-14 get DOAJ, cf. #6524, #4983
        shellout("""span-export -doi-blacklist {blacklist} -any DE-540 -l DE-15-FID:{issns} -any DE-15 -any DE-14 <(unpigz -c {input}) | pigz >> {output}""",
                 blacklist=self.input().get('blacklist').path, input=self.input().get('doaj').path, issns=self.input().get('DE-15-FID').path, output=stopover)

        # DE-15 gets DeGruyter, cf. #4731
        shellout("""span-export -doi-blacklist {blacklist} -skip -f DE-15:{holding} <(unpigz -c {input}) | pigz >> {output}""",
                 blacklist=self.input().get('blacklist').path, holding=self.input().get('DE-15').path, input=self.input().get('degruyter').path, output=stopover)

        def format_args(flagname, isils):
            """ Helper to create args like -f DE-15:/path/to/target -f DE-14:/path/to/target ..."""
            args = ["%s %s:%s" % (flagname, isil, self.input().get(isil).path) for isil in isils]
            return " ".join(args)

        files = format_args("-f", ['DE-105', 'DE-14', 'DE-15', 'DE-Bn3', 'DE-Ch1', 'DE-Gla1', 'DE-Zi4', 'DE-J59'])
        lists = format_args("-l", ['DE-15-FID'])

        # apply holdings and issn filters on sources
        for source in ['crossref', 'jstor']:
            shellout("span-export -doi-blacklist {blacklist} -skip {files} {lists} <(unpigz -c {input}) | pigz -c >> {output}",
                     blacklist=self.input().get('blacklist').path, files=files, lists=lists,
                     input=self.input().get(source).path, output=stopover)

        luigi.File(stopover).move(self.output().path)

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

class AICoverage(AITask):
    """
    Ad-hoc task to run coverage tests with new [iscov](https://github.com/miku/istools).
    """
    date = ClosestDateParameter(default=datetime.date.today())
    file = luigi.Parameter(description='path to holding file')
    format = luigi.Parameter(default='kbart', description='holding file format: kbart, ovid, google')

    def requires(self):
        return {
            'crossref': CrossrefIntermediateSchema(date=self.date),
            'degruyter': DegruyterIntermediateSchema(date=self.date),
            'jstor' : JstorIntermediateSchema(date=self.date),
            'gbi-references': GBIIntermediateSchemaByKind(kind='references')
        }

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for k, v in self.input().iteritems():
            shellout(r"""iscov -file {file} -format {format} <(unpigz -c {input}) | awk '{{ print $0"\t{key}" }}' >> {output}""",
                     file=self.file, format=self.format, input=v.path, key=k, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True), format=TSV)

class AICoverageReport(AITask):
    """
    Human readable coverage report.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    file = luigi.Parameter(description='path to holding file')
    format = luigi.Parameter(default='kbart', description='holding file format: kbart, ovid, google')

    def requires(self):
        return AICoverage(date=self.date, file=self.file, format=self.format)

    def run(self):
        counter = collections.Counter()
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('id', 'valid', 'source')):
                counter["%s:%s" % (row.source, row.valid)] += 1
                counter[row.source] += 1

        sources = set()
        for k, v in counter.iteritems():
            if ":" in k:
                continue
            sources.add(k)

        with self.output().open('w') as output:
            s = "Coverage report for %s (%s) as of %s" % (self.file, self.format, self.date)
            output.write("\n" + s + "\n")
            output.write("=" * len(s) + "\n\n")
            summary = collections.Counter()
            for source in sorted(sources):
                total = counter[source]
                valid = counter["%s:true" % source]
                covered = 100.0 / total * valid
                output.write("* %s is %0.2f%% covered (%s/%s)\n" % (source, covered, valid, total))

                summary["total"] += total
                summary["valid"] += valid

            covered = 100.0 / summary["total"] * summary["valid"]
            output.write("\nCoverage across sources: %0.2f%% (%s/%s)" % (covered, summary["valid"], summary["total"]))
            output.write("\n\n")
            output.write("No liability assumed.\n")

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True), format=TSV)
