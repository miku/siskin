# coding: utf-8
# pylint: disable=C0301,E1101,C0330,C0111

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
Electronic Resource Management System Based on Linked Data Technologies.

https://amsl.technology

> Managing electronic resources has become a distinctive and important task for
libraries in recent years. The diversity of resources, changing licensing
policies and new business models of the publishers, consortial acquisition and
modern web scale discovery technologies have turned the market place of
scientific information into a complex and multidimensional construct.

These tasks wrap API responses, allow access to holding files, and create a
filterconfig which can be use fed into
[span-tag](https://github.com/miku/span/tree/master/cmd/span-tag).

Config:

[amsl]

base = https://x.y.z
uri-download-prefix = https://x.y.z/OntoWiki/files/get?setResource=
write-url = https://x.y.z/OntoWiki/x/y # For time-stamping.

"""

from __future__ import print_function

import collections
import datetime
import gzip
import io
import itertools
import json
import operator
import tempfile
import zipfile

import luigi
from gluish.format import TSV, Gzip
from gluish.utils import shellout

from siskin.task import DefaultTask
from siskin.utils import SetEncoder, dictcheck

class AMSLTask(DefaultTask):
    """
    Base class for AMSL related tasks.
    """
    TAG = 'amsl'

class AMSLServiceDeprecated(AMSLTask):
    """
    Defunkt task via #14415 as of 2018-12-12. Will be remove soon.

    Retrieve AMSL API response. Outbound: holdingsfiles, contentfiles, metadata_usage.
    2018-12-12: discovery API EOL, XXX: adjust, refs #14415.

    Example output (discovery):

        [
            {
                "shardLabel": "SLUB-dbod",
                "sourceID": "64",
                "megaCollection": "Perinorm – Datenbank für Normen und technische Regeln",
                "productISIL": null,
                "externalLinkToContentFile": null,
                "contentFileLabel": null,
                "contentFileURI": null,
                "linkToContentFile": null,
                "ISIL": "DE-105",
                "evaluateHoldingsFileForLibrary": "no",
                "holdingsFileLabel": null,
                "holdingsFileURI": null,
                "linkToHoldingsFile": null
            },
            {
                "shardLabel": "SLUB-dbod",
                "sourceID": "64",
                "megaCollection": "Perinorm – Datenbank für Normen und technische Regeln",
                "productISIL": null,
                "externalLinkToContentFile": null,
                "contentFileLabel": null,
                "contentFileURI": null,
                "linkToContentFile": null,
                "ISIL": "DE-14",
                "evaluateHoldingsFileForLibrary": "no",
                "holdingsFileLabel": null,
                "holdingsFileURI": null,
                "linkToHoldingsFile": null
            },
        ...

    """
    date = luigi.DateParameter(default=datetime.date.today())
    name = luigi.Parameter(default='outboundservices:discovery',
                           description='discovery, holdingsfiles, contentfiles, metadata_usage, freeContent')

    def run(self):
        parts = self.name.split(':')
        if not len(parts) == 2:
            raise RuntimeError('realm:name expected, e.g. outboundservices:discovery')
        realm, name = parts

        link = '%s/%s/list?do=%s' % (
            self.config.get('amsl', 'base').rstrip('/'), realm, name)
        output = shellout("""curl --fail "{link}" | pigz -c > {output} """, link=link)

        # Check for valid JSON before, simplifies debugging.
        with gzip.open(output, 'rb') as handle:
            try:
                _ = json.load(handle)
            except ValueError as err:
                self.logger.warning("AMSL API did not return valid JSON")
                raise

        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True, ext='json.gz'), format=Gzip)

class AMSLService(AMSLTask):
    """
    Approximation of discovery response until better approach is implemented. Requires span 0.1.273 or later.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    name = luigi.Parameter(default='outboundservices:discovery',
                           description='ignored, kept for compatibility')

    def requires(self):
        if self.name != 'outboundservices:discovery':
            return AMSLServiceDeprecated(date=self.date, name=self.name)
        return []

    def run(self):
        if self.name == 'outboundservices:discovery':
            output = shellout("span-amsl-discovery -live {live} | gzip -c > {output}",
                              live=self.config.get('amsl', 'base'))
        else:
            output = self.input().path

        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True, ext='json.gz'), format=Gzip)


class AMSLCollections(AMSLTask):
    """
    Report all collections, that appear in AMSL.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return AMSLService(date=self.date, name='outboundservices:discovery')

    def run(self):
        output = shellout(""" unpigz -c {input} | jq -rc '.[]|.megaCollection' | sort -u > {output}""", input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class AMSLCollectionsShardFilter(AMSLTask):
    """
    A per-shard list of collection entries. One record per line.

        {
          "evaluateHoldingsFileForLibrary": "no",
          "holdingsFileLabel": null,
          "megaCollection": "DOAJ Directory of Open Access Journals",
          "shardLabel": "UBL-ai",
          "contentFileURI": null,
          "sourceID": "28",
          "linkToHoldingsFile": null,
          "holdingsFileURI": null,
          "productISIL": null,
          "linkToContentFile": null,
          "contentFileLabel": null,
          "externalLinkToContentFile": null,
          "ISIL": "DE-14"
        }
        ....

    Shard distribution as of January 2017:

        $ taskcat AMSLService | jq -rc '.[] | .shardLabel' | sort | uniq -c | sort -nr
        53493 UBL-ai
         1121 UBL-main
          245 SLUB-dswarm
           19 SLUB-dbod

    """
    date = luigi.DateParameter(default=datetime.date.today())
    shard = luigi.Parameter(default='UBL-ai',
                            description='only collect items for this shard')

    def requires(self):
        return AMSLService(date=self.date, name='outboundservices:discovery')

    def run(self):
        with self.input().open() as handle:
            doc = json.load(handle)

        with self.output().open('w') as output:
            for item in doc:
                if not item['shardLabel'] == self.shard:
                    continue
                output.write(json.dumps(item) + "\n")

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class AMSLCollectionsISILList(AMSLTask):
    """
    A per-shard list of ISILs for which AMSL has some information.

        DE-105
        DE-14
        DE-15
        DE-15-FID
        DE-1972
        DE-540
        ...
    """
    date = luigi.DateParameter(default=datetime.date.today())
    shard = luigi.Parameter(default='UBL-ai',
                            description='only collect items for this shard')

    def requires(self):
        return AMSLService(date=self.date, name='outboundservices:discovery')

    def run(self):
        with self.input().open() as handle:
            doc = json.load(handle)

        isils = set()

        for item in doc:
            if not item['shardLabel'] == self.shard:
                continue
            isils.add(item['ISIL'])

        if len(isils) == 0:
            raise RuntimeError('no isils found: maybe mispelled shard name?')

        with self.output().open('w') as output:
            for isil in sorted(isils):
                output.write_tsv(isil)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class AMSLCollectionsISIL(AMSLTask):
    """
    Per ISIL list of collections.

        {
          "48": [
            "Genios (Recht)",
            "Genios (Sozialwissenschaften)",
            "Genios (Psychologie)",
            "Genios (Fachzeitschriften)",
            "Genios (Wirtschaftswissenschaften)"
          ],
          "49": [
            "Helminthological Society (CrossRef)",
            "International Association of Physical Chemists (IAPC) (CrossRef)",
            ...

    Examples (Jan 2017):

        $ taskcat AMSLCollectionsISIL --isil DE-Mit1 --shard SLUB-dbod | jq .
        {
            "64": [
                "Perinorm – Datenbank für Normen und technische Regeln"
            ]
        }

        $ taskcat AMSLCollectionsISIL --isil DE-14 --shard SLUB-dswarm | jq .
        {
            "83": [
                "SLUB/Mediathek"
            ],
            "69": [
                "Wiley ebooks"
            ],
            "105": [
                "Springer Journals"
            ],
            "94": [
                "Blackwell Publishing Journal Backfiles 1879-2005",
                "China Academic Journals (CAJ) Archiv",
                "Wiley InterScience Backfile Collections 1832-2005",
                "Torrossa / Periodici",
                "Cambridge Journals Digital Archive",
                "Elsevier Journal Backfiles on ScienceDirect",
                "Periodicals Archive Online",
                "Emerald Fulltext Archive Database",
                "Springer Online Journal Archives"
            ],
            "67": [
                "SLUB/Deutsche Fotothek"
            ]
        }

        $ taskcat AMSLCollectionsISIL --isil DE-15 --shard SLUB-dswarm | jq .
        {
            "68": [
                "OLC SSG Medien- / Kommunikationswissenschaft",
                "OLC SSG Film / Theater"
            ],
            "94": [
                "Blackwell Publishing Journal Backfiles 1879-2005",
                "China Academic Journals (CAJ) Archiv",
                "Wiley InterScience Backfile Collections 1832-2005",
                "Torrossa / Periodici",
                "Cambridge Journals Digital Archive",
                "Elsevier Journal Backfiles on ScienceDirect",
                "Periodicals Archive Online",
                "Emerald Fulltext Archive Database",
                "Springer Online Journal Archives"
            ],
            "105": [
                "Springer Journals"
            ]
        }
    """
    date = luigi.DateParameter(default=datetime.date.today())
    isil = luigi.Parameter(default='DE-15', description='ISIL, case sensitive')
    shard = luigi.Parameter(default='UBL-ai',
                            description='only collect items for this shard')

    def requires(self):
        return AMSLService(date=self.date, name='outboundservices:discovery')

    def run(self):
        with self.input().open() as handle:
            doc = json.load(handle)

        unique_shards = set()

        scmap = collections.defaultdict(set)
        for item in doc:
            unique_shards.add(item['shardLabel'])
            if not item['shardLabel'] == self.shard:
                continue
            if not item['ISIL'] == self.isil:
                continue
            scmap[item['sourceID']].add(item['megaCollection'].strip())
            if not scmap:
                raise RuntimeError('no collections found for ISIL: %s' % self.isil)

        if not scmap and self.shard not in unique_shards:
            self.logger.warn('available shards: %s', list(unique_shards))

        with self.output().open('w') as output:
            output.write(json.dumps(scmap, cls=SetEncoder) + "\n")

    def output(self):
        return luigi.LocalTarget(path=self.path())

class AMSLHoldingsFile(AMSLTask):
    """
    Access AMSL files/get?setResource= facilities.

    The output is probably zipped (will be decompressed on the fly).

    One ISIL can have multiple files (they will be concatenated).

    Output should be in standard KBART format, given the uploaded files in AMSL are KBART.
    """
    isil = luigi.Parameter(default='DE-15', description='ISIL, case sensitive')
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return AMSLService(date=self.date, name='outboundservices:holdingsfiles')

    def run(self):
        with self.input().open() as handle:
            holdings = json.load(handle)

        _, stopover = tempfile.mkstemp(prefix='siskin-')

        # The property containing the URI of the holding file, maybe.
        urikey = 'DokumentURI'

        for holding in holdings:
            if holding["ISIL"] == self.isil:

                if urikey not in holding:
                    raise RuntimeError('possible AMSL API change, expected: %s, available keys: %s' % (
                        urikey, list(holding.keys())))

                # refs. #7142
                if 'kbart' not in holding[urikey].lower():
                    self.logger.debug("skipping non-KBART holding URI: %s", holding[urikey])
                    continue

                link = "%s%s" % (self.config.get('amsl', 'uri-download-prefix'), holding[urikey])
                downloaded = shellout("curl --fail {link} > {output} ", link=link)
                try:
                    _ = zipfile.ZipFile(downloaded)
                    shellout("unzip -p {input} >> {output}",
                             input=downloaded, output=stopover)
                except zipfile.BadZipfile:
                    # Probably not a zip.
                    shellout("cat {input} >> {output}",
                             input=downloaded, output=stopover)

        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

class AMSLOpenAccessISSNList(AMSLTask):
    """
    List of ISSN, which are open access.

    For now, extract these ISSN from a special holding file, called
    KBART_FREEJOURNALS via AMSL.

    Example (Jan 2017):

        $ taskcat AMSLOpenAccessISSNList | head -10
        0001-0944
        0001-1843
        0001-186X
        0001-1983
        0001-2114
        0001-2211
        0001-267X
        0001-3714
        0001-3757
        0001-3765

    As of October 2017, this list includes: https://pub.uni-bielefeld.de/download/2913654/2913655.
    As of December 2018, there are 72692 ISSN listed.
    """

    date = luigi.DateParameter(default=datetime.date.today())

    def run(self):
        """
        Download, maybe unzip, grab column 2 and 3, keep ISSN, sort -u.
        """
        key = "http://amsl.technology/discovery/metadata-usage/Dokument/KBART_FREEJOURNALS"
        link = "%s%s" % (self.config.get('amsl', 'uri-download-prefix'), key)

        downloaded = shellout("curl --fail {link} > {output} ", link=link)
        _, stopover = tempfile.mkstemp(prefix='siskin-')

        try:
            _ = zipfile.ZipFile(downloaded)
            output = shellout("unzip -p {input} >> {output}", input=downloaded)
        except zipfile.BadZipfile:
            # At least the file is not a zip.
            output = shellout("cat {input} >> {output}", input=downloaded)

        shellout("cut -f 2 {input} | grep -oE '[0-9]{{4,4}}-[xX0-9]{{4,4}}' >> {output}",
                 input=output, output=stopover)
        shellout("cut -f 3 {input} | grep -oE '[0-9]{{4,4}}-[xX0-9]{{4,4}}' >> {output}",
                 input=output, output=stopover)

        # Include OA list, refs #11579, maybe cache this?
        shellout("""curl -s https://pub.uni-bielefeld.de/download/2913654/2913655 | cut -d, -f1,2 | tr -d '"' |
                    grep -E '[0-9]{{4,4}}-[0-9]{{3,3}}[0-9xX]' | tr ',' '\n' >> {output}""",
                 output=stopover, preserve_whitespace=True)

        output = shellout("sort -u {input} > {output}", input=stopover)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

class AMSLGoldListKBART(AMSLTask):
    """
    Convert Bielefeld Gold List to KBART (for manual uploads in AMSL). As of
    December 2018, there are 36706 ISSN in this list.
    """

    date = luigi.DateParameter(default=datetime.date.today())

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout(""" echo "online_identifier" > {output}""", output=stopover)
        # Include OA list, refs #11579.
        shellout("""curl -s https://pub.uni-bielefeld.de/download/2913654/2913655 |
                    cut -d, -f1,2 |
                    tr -d '"' |
                    grep -E '[0-9]{{4,4}}-[0-9]{{3,3}}[0-9xX]' |
                    tr ',' '\n' |
                    sort -u |
                    grep -v ^$ >> {output}""",
                 output=stopover, preserve_whitespace=True)
        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

class AMSLFreeContent(AMSLTask):
    """
    Free content. Revelant for OA flags.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("curl -s '{base}/inhouseservices/list?do=freeContent' | jq -rc . > {output}",
                          base=self.config.get('amsl', 'base'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

class AMSLOpenAccessKBART(AMSLTask):
    """
    Create a KBART file that contains open access and freely available journals only.

    Used in conjunction with [span-oa-filter](https://git.io/vdB29).
    """

    date = luigi.DateParameter(default=datetime.date.today())

    def run(self):
        """
        Download, maybe unzip, combine with Gold List.
        """
        key = "http://amsl.technology/discovery/metadata-usage/Dokument/KBART_FREEJOURNALS"
        link = "%s%s" % (self.config.get('amsl', 'uri-download-prefix'), key)

        downloaded = shellout("curl --fail {link} > {output} ", link=link)
        _, stopover = tempfile.mkstemp(prefix='siskin-')

        try:
            _ = zipfile.ZipFile(downloaded)
            output = shellout("unzip -p {input} >> {output}", input=downloaded)
        except zipfile.BadZipfile:
            # At least the file is not a zip.
            output = shellout("cat {input} >> {output}", input=downloaded)

        # Include OA list, refs #11579.
        shellout("""curl -s https://pub.uni-bielefeld.de/download/2913654/2913655 | cut -d, -f1,2 | tr -d '"' |
                    grep -E '[0-9]{{4,4}}-[0-9]{{3,3}}[0-9xX]' | tr ',' '\n' |
                    awk '{{ print "\t\t"$0 }}' >> {output}""", output=output, preserve_whitespace=True)

        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

class AMSLWisoPackages(AMSLTask):
    """
    Collect WISO packages.

    XXX(miku): Throw this away.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    encoding = luigi.Parameter(default='latin-1', description='wiso journal id csv file encoding', significant=False)

    def requires(self):
        return AMSLService(date=self.date)

    def hardcoded_list_of_wiso_journal_identifiers(self):
        """
        Refs. #10707, Att. #4444.
        """
        ids = set()
        filename = self.assets('wiso/645896059854847ce4ccd1416e11ba372e45bfd6.csv')
        with io.open(filename, encoding=self.encoding) as handle:
            for i, line in enumerate(handle):
                if i == 0:
                    continue
                line = line.strip()
                if not line:
                    continue
                fields = line.split(';')
                if len(fields) < 12:
                    raise ValueError('expected ; separated KBART-ish file with journal identifier at column 11')
                id = fields[11].strip()
                if not id:
                    continue
                ids.add(id)

        return sorted(ids)

    def resolve_ubl_profile(self, colls):
        """
        Given a list of collection names, replace the complete list with
        hardcoded WISO journal identifiers from #10707/4444.
        """
        if 'wiso UB Leipzig Profil' in colls:
            return self.hardcoded_list_of_wiso_journal_identifiers()

        return colls

    def run(self):
        with self.input().open() as handle:
            doc = json.loads(handle.read())

        isilpkg = collections.defaultdict(lambda: collections.defaultdict(set))

        for item in doc:
            isil, sid = item.get('ISIL'), item.get('sourceID')
            mega_collection = item.get('megaCollection')
            lthf = item.get('linkToHoldingsFile')
            if sid != "48":
                continue
            isilpkg[isil][lthf].add(mega_collection)

        filterconfig = collections.defaultdict(dict)
        fzs_package_name = 'Genios (Fachzeitschriften)'

        for isil, blob in list(isilpkg.items()):
            include_fzs = False
            for _, colls in list(blob.items()):
                if fzs_package_name in colls:
                    include_fzs = True

            filters = []

            if include_fzs and isil != 'DE-15-FID':
                packages = set(itertools.chain(
                    *[c for _, c in list(blob.items())]))
                packages = self.resolve_ubl_profile(packages)
                filters.append({
                    'and': [
                        {'source': ['48']},
                        {'package': [fzs_package_name]},
                        {'package': [
                            name for name in packages if name != fzs_package_name]}
                    ]
                })

            for lthf, colls in list(blob.items()):
                if lthf is None or lthf == 'null':
                    continue
                if isil == 'DE-15-FID':
                    colls = self.resolve_ubl_profile(colls)
                    filter = {
                        'and': [
                            {'source': ['48']},
                            {'holdings': {'urls': [lthf]}},
                            {'package': [c for c in colls if c !=
                                         fzs_package_name]},
                        ]
                    }
                else:
                    filter = {
                        'and': [
                            {'source': ['48']},
                            {'holdings': {'urls': [lthf]}},
                            {'package': [c for c in colls if c !=
                                         fzs_package_name]},
                            {'not': {'package': [fzs_package_name]}}
                        ]
                    }
                filters.append(filter)
            filterconfig[isil] = {'or': filters}

        with self.output().open('w') as output:
            json.dump(filterconfig, output, cls=SetEncoder)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='json'))

class AMSLFilterConfigFreeze(AMSLTask):
    """
    Create a frozen file. File will contain the filterconfig plus content of all URLs.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return AMSLFilterConfig(date=self.date)

    def run(self):
        output = shellout("span-freeze -o {output} < {input}", input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='zip'))

class AMSLFilterConfig(AMSLTask):
    """
    Turn AMSL API to a span(1) filter configuration.

    Cases (Spring 2017):

    61084 Case 3 (ISIL, SID, Collection, Holding File)
      805 Case 2 (ISIL, SID, Collection, Product ISIL)
      380 Case 1 (ISIL, SID, Collection)
       38 Case 4 (ISIL, SID, Collection, External Content File)
       31 Case 6 (ISIL, SID, Collection, External Content File, Holding File)
        1 Case 7 (ISIL, SID, Collection, Internal Content File, Holding File)
        1 Case 5 (ISIL, SID, Collection, Internal Content File)

    Process:

                AMSL Discovery API
                      |
                      v
                AMSLFilterConfig
                      |
                      v
        $ span-tag -c config.json < input.is > output.is

    Notes:

    This task turns an AMSL discovery API response into a filterconfig[1], which
    span(1) can understand.

    AMSL API might not specify everything we need to know, so this task shall
    be the only place, where workarounds happen.

    Whil span-tag is fast, it is not fast enough to iterate over a disjuction
    of 60K items for each of the 100M documents fast enough, which - if we
    could - would simplify the implementation of this task.

    The main speed improvement comes from using lists of collection names
    instead of having each collection processed separately - which is how it
    works conceptually: Each collection could use a separate KBART file (or use
    none at all).

    We ignore collection names, if (external) content files are used. These
    content files are usually there, because we cannot infer the collection
    name from the data alone.

    Performance data point: 22 ISIL each with between 1 and 26 alternatives for
    attachment, each alternative consisting of around three filters. Around 30
    holding or content files each with between 10 and 50000 entries referenced
    about 200 times in total: around 20k records/s.

    Case table (Feb 2019), X/-/o, yes, no, maybe.

    SID COLL ISIL LTHF LTCF ELTCF PI TCID
    -------------------------------------
    X   X    X    -    -    -     -   o
    X   X    X    X    -    -     -   o
    X   X    X    X    X    -     -   o
    X   X    X    X    -    X     -   o
    X   X    X    X    -    -     X   o
    X   X    X    -    X    -     -   o
    X   X    X    -    -    X     -   o
    X   X    X    -    -    -     X   o

    ----

    [1] https://git.io/vQohE
    """

    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'amsl': AMSLService(date=self.date),
            'wiso': AMSLWisoPackages(date=self.date),
        }

    def run(self):
        with self.input().get('amsl').open() as handle:
            doc = json.loads(handle.read())

        # Case: ISIL, SID, collection.
        isilsidcollections = collections.defaultdict(
            lambda: collections.defaultdict(set))

        # Case: ISIL, SID, collection, link.
        isilsidlinkcollections = collections.defaultdict(
            lambda: collections.defaultdict(
                lambda: collections.defaultdict(set)))

        # Ready-made filters per ISIL. Some filters can be added on-the-fly
        # because there aren't many occurences.
        isilfilters = collections.defaultdict(list)

        for item in doc:
            isil, sid, mega_collection, technicalCollectionID = operator.itemgetter(
                'ISIL', 'sourceID', 'megaCollection', 'technicalCollectionID')(item)

            if sid == '48':  # Handled elsewhere.
                continue

            # refs #10495, a subject filter for a few hard-coded ISIL.
            if sid == '34' and isil in ('DE-L152', 'DE-1156', 'DE-1972', 'DE-Kn38'):
                isilfilters[isil].append({
                    "and": [
                        {
                            "source": ["34"],
                        },
                        {
                            "subject": [
                                "Music",
                                "Music education",
                            ]
                        },
                    ]
                })
                continue

            # refs #10495, maybe use a TSV with custom column name to use a subject list?
            if sid == '34' and isil == 'DE-15-FID':
                isilfilters[isil].append({
                    "and": [
                        {
                            "source": ["34"],
                        },
                        {
                            "subject": [
                                "Film studies",
                                "Information science",
                                "Mass communication",
                            ]
                        },
                    ]
                })
                continue

            # SID COLL ISIL LTHF LTCF ELTCF PI TCID
            # -------------------------------------
            # X   X    X    -    -    -     -   o
            if dictcheck(item, contains=['sourceID', 'megaCollection', 'ISIL'],
                         absent=['linkToHoldingsFile',
                                  'linkToContentFile',
                                  'externalLinkToContentFile',
                                  'productISIL'],
                         ignore=['technicalCollectionID']):

                isilsidcollections[isil][sid].add(mega_collection)
                if technicalCollectionID:
                    isilsidcollections[isil][sid].add(technicalCollectionID)

            # SID COLL ISIL LTHF LTCF ELTCF PI TCID
            # -------------------------------------
            # X   X    X    -    -    -     X   o
            elif dictcheck(item, contains=['sourceID', 'megaCollection', 'ISIL', 'productISIL'],
                           absent=['linkToHoldingsFile', 'linkToContentFile', 'externalLinkToContentFile'],
                           ignore=['technicalCollectionID']):

                isilsidcollections[isil][sid].add(mega_collection)
                self.logger.debug("productISIL given, but ignored: %s, %s, %s", isil, sid, item['productISIL'])
                if technicalCollectionID:
                    isilsidcollections[isil][sid].add(technicalCollectionID)

            # SID COLL ISIL LTHF LTCF ELTCF PI TCID
            # -------------------------------------
            # X   X    X    X    -    -     X   o
            elif dictcheck(item, contains=['sourceID',
                                           'megaCollection',
                                           'ISIL',
                                           'linkToHoldingsFile',
                                           'productISIL'],
                           absent=['linkToContentFile', 'externalLinkToContentFile'],
                           ignore=['technicalCollectionID']):

                self.logger.debug("productISIL is set, but we do not have a filter for it yet: %s, %s, %s",
                                  isil, sid, mega_collection)

                if item.get('evaluateHoldingsFileForLibrary') == "yes":
                    isilsidlinkcollections[isil][sid][item['linkToHoldingsFile']].add(
                        mega_collection)
                    if technicalCollectionID:
                        isilsidlinkcollections[isil][sid][item['linkToHoldingsFile']].add(technicalCollectionID)
                else:
                    self.logger.warning(
                        "evaluateHoldingsFileForLibrary=no plus link: skipping %s", item)

            # SID COLL ISIL LTHF LTCF ELTCF PI TCID
            # -------------------------------------
            # X   X    X    X    -    -     -   o
            elif dictcheck(item, contains=['sourceID', 'megaCollection', 'ISIL', 'linkToHoldingsFile'],
                           absent=['linkToContentFile', 'externalLinkToContentFile', 'productISIL'],
                           ignore=['technicalCollectionID']):

                if item.get('evaluateHoldingsFileForLibrary') == "yes":
                    isilsidlinkcollections[isil][sid][item['linkToHoldingsFile']].add(
                        mega_collection)
                    if technicalCollectionID:
                        isilsidlinkcollections[isil][sid][item['linkToHoldingsFile']].add(technicalCollectionID)
                else:
                    self.logger.warning(
                        "evaluateHoldingsFileForLibrary=no plus link: skipping %s", item)

            # SID COLL ISIL LTHF LTCF ELTCF PI TCID
            # -------------------------------------
            # X   X    X    -    -    X     -   o
            elif dictcheck(item, contains=['sourceID', 'megaCollection', 'ISIL', 'externalLinkToContentFile'],
                           absent=['linkToHoldingsFile', 'linkToContentFile', 'productISIL'],
                           ignore=['technicalCollectionID']):

                isilfilters[isil].append({
                    "and": [
                        {"source": [sid]},
                        {"holdings": {
                            "urls": [item["externalLinkToContentFile"]]}},
                    ]})

            # SID COLL ISIL LTHF LTCF ELTCF PI TCID
            # -------------------------------------
            # X   X    X    -    X    -     -   o
            elif dictcheck(item, contains=['sourceID',
                                           'megaCollection',
                                           'ISIL',
                                           'linkToContentFile'],
                           absent=['linkToHoldingsFile', 'externalLinkToContentFile', 'productISIL'],
                           ignore=['technicalCollectionID']):

                isilfilters[isil].append({
                    "and": [
                        {"source": [sid]},
                        {"holdings": {"urls": [item["linkToContentFile"]]}},
                    ]})

            # SID COLL ISIL LTHF LTCF ELTCF PI TCID
            # -------------------------------------
            # X   X    X    X    -    X     -   o
            elif dictcheck(item, contains=['sourceID',
                                           'megaCollection',
                                           'ISIL',
                                           'linkToHoldingsFile',
                                           'externalLinkToContentFile'],
                           absent=['linkToContentFile', 'productISIL'],
                           ignore=['technicalCollectionID']):

                if item.get('evaluateHoldingsFileForLibrary') == "yes":
                    isilfilters[isil].append({
                        "and": [
                            {"source": [sid]},
                            {"holdings": {
                                "urls": [item["externalLinkToContentFile"]]}},
                            {"holdings": {
                                "urls": [item["linkToHoldingsFile"]]}},
                        ]})
                else:
                    self.logger.warning(
                        "evaluateHoldingsFileForLibrary=no plus link: skipping %s", item)

            # SID COLL ISIL LTHF LTCF ELTCF PI TCID
            # -------------------------------------
            # X   X    X    X    X    -     -   o
            elif dictcheck(item, contains=['sourceID',
                                           'megaCollection',
                                           'ISIL',
                                           'linkToHoldingsFile',
                                           'linkToContentFile'],
                           absent=['externalLinkToContentFile', 'productISIL'],
                           ignore=['technicalCollectionID']):

                if item.get('evaluateHoldingsFileForLibrary') == "yes":
                    isilfilters[isil].append({
                        "and": [
                            {"source": [sid]},
                            {"holdings": {
                                "urls": [item["linkToContentFile"]]}},
                            {"holdings": {
                                "urls": [item["linkToHoldingsFile"]]}},
                        ]})
                else:
                    self.logger.warning(
                        "evaluateHoldingsFileForLibrary=no plus link: skipping %s", item)

            # SID COLL ISIL LTHF LTCF ELTCF PI TCID
            # -------------------------------------
            # ?   ?    ?    ?    ?    ?     ?   o
            else:
                raise RuntimeError(
                    "unhandled combination of sid, collection and other parameters: %s", item)

        # A second pass.
        for isil, blob in list(isilsidcollections.items()):
            for sid, colls in list(blob.items()):
                isilfilters[isil].append({
                    "and": [
                        {"source": [sid]},
                        {"collection": sorted(colls)},
                    ]
                })

        # A second pass.
        for isil, blob in list(isilsidlinkcollections.items()):
            for sid, spec in list(blob.items()):
                for link, colls in list(spec.items()):
                    isilfilters[isil].append({
                        "and": [
                            {"source": [sid]},
                            {"collection": sorted(colls)},
                            {"holdings": {"urls": [link]}},
                        ]
                    })

        # Final assembly.
        filterconfig = collections.defaultdict(dict)
        for isil, filters in list(isilfilters.items()):
            if len(filters) == 0:
                continue
            if len(filters) == 1:
                filterconfig[isil] = filters[0]
                continue
            filterconfig[isil] = {"or": filters}

        # Include WISO.
        with self.input().get('wiso').open() as handle:
            wisoconf = json.load(handle)
            for isil, tree in list(wisoconf.items()):
                for filter in tree.get('or', []):
                    filterconfig[isil]['or'].append(filter)

        # XXX: Adjust a few items for DE-14, cf. 2018-06-11, namely, add links
        # to external holding files, which are not included into the AMSL
        # discovery API response, refs #13378.
        fix_url = 'https://dbod.de/SLUB-EZB-KBART.zip'
        for term in filterconfig["DE-14"]["or"]:
            for t in term["and"]:
                if (not 'holdings' in t) and (not 'urls' in t.get('holdings', [])):
                    continue
                if fix_url in t['holdings']['urls']:
                    continue
                t['holdings']['urls'].append("https://dbod.de/SLUB-EZB-KBART.zip")

        with self.output().open('w') as output:
            json.dump(filterconfig, output, cls=SetEncoder)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='json'))
