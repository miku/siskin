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

"""
Electronic Resource Management System Based on Linked Data Technologies.

http://amsl.technology

Config:

[amsl]

uri-download-prefix = https://x.y.z/OntoWiki/files/get?setResource=
base = https://example.com
fid-issn-list = https://goo.gl/abcdef

"""

import collections
import datetime
import json
import tempfile
import zipfile

import luigi

from gluish.format import TSV
from gluish.utils import shellout
from siskin.task import DefaultTask
from siskin.utils import SetEncoder


class AMSLTask(DefaultTask):
    TAG = 'amsl'

class AMSLService(AMSLTask):
    """
    Retrieve AMSL API response. Outbound: discovery, holdingsfiles,
    contentfiles, metadata_usage.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    name = luigi.Parameter(default='outboundservices:discovery',
                           description='discovery, holdingsfiles, contentfiles, metadata_usage')

    def run(self):
        parts = self.name.split(':')
        if not len(parts) == 2:
            raise RuntimeError('name must be of the form realm:name, e.g. outboundservices:discovery')
        realm, name = parts

        link = '%s/%s/list?do=%s' % (self.config.get('amsl', 'base').rstrip('/'), realm, name)
        output = shellout("""curl --fail "{link}" > {output} """, link=link)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True))

class AMSLCollectionsShardFilter(AMSLTask):
    """
    A per-shard list of collection entries. One record per line.

        {
          "evaluateHoldingsFileForLibrary": "no",
          "holdingsFileLabel": null,
          "collectionLabel": "DOAJ Directory of Open Access Journals",
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

    """
    date = luigi.DateParameter(default=datetime.date.today())
    shard = luigi.Parameter(default='UBL-ai', description='only collect items for this shard')

    def requires(self):
        return AMSLService(date=self.date, name='outboundservices:discovery')

    def run(self):
        with self.input().open() as handle:
            c = json.load(handle)

        with self.output().open('w') as output:
            for item in c:
                if not item['shardLabel'] == self.shard:
                    continue
                output.write(json.dumps(item) + "\n")

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class AMSLCollectionsISILList(AMSLTask):
    """
    A per-shard list of ISILs for which we get information from AMSL.

        DE-105
        DE-14
        DE-15
        DE-15-FID
        DE-1972
        DE-540
        ...

    """
    date = luigi.DateParameter(default=datetime.date.today())
    shard = luigi.Parameter(default='UBL-ai', description='only collect items for this shard')

    def requires(self):
        return AMSLService(date=self.date, name='outboundservices:discovery')

    def run(self):
        with self.input().open() as handle:
            c = json.load(handle)

        isils = set()

        for item in c:
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

    """
    date = luigi.DateParameter(default=datetime.date.today())
    isil = luigi.Parameter(description='ISIL, case sensitive')
    shard = luigi.Parameter(default='UBL-ai', description='only collect items for this shard')

    def requires(self):
        return AMSLService(date=self.date, name='outboundservices:discovery')

    def run(self):
        with self.input().open() as handle:
            c = json.load(handle)
        scmap = collections.defaultdict(set)
        for item in c:
            if not item['shardLabel'] == self.shard:
                continue
            if not item['ISIL'] == self.isil:
                continue
            scmap[item['sourceID']].add(item['collectionLabel'].strip())
        if not scmap:
            raise RuntimeError('no collections found for ISIL: %s' % self.isil)

        with self.output().open('w') as output:
            output.write(json.dumps(scmap, cls=SetEncoder) + "\n")

    def output(self):
        return luigi.LocalTarget(path=self.path())

class AMSLHoldingsFile(AMSLTask):
    """
    Access AMSL files/get?setResource= facilities.

    The output is probably zipped (will be decompressed on the fly).

    One ISIL can have multiple files (they will be combined).
    """
    isil = luigi.Parameter(description='ISIL, case sensitive')
    date = luigi.Parameter(default=datetime.date.today())

    def requires(self):
        return AMSLService(date=self.date, name='outboundservices:holdingsfiles')

    def run(self):
        with self.input().open() as handle:
            holdings = json.load(handle)

        _, stopover = tempfile.mkstemp(prefix='siskin-')

        # The property which contains the URI of the holding file. Might change.
        urikey = 'DokumentURI'

        for holding in holdings:
            if holding["ISIL"] == self.isil:

                if urikey not in holding:
                    raise RuntimeError('possible AMSL API change, expected: %s, available keys: %s' % (urikey, holding.keys()))

                # refs. #7142
                if 'kbart' not in holding[urikey].lower():
                    self.logger.debug("skipping non-KBART holding URI: %s" % holding[urikey])
                    continue

                link = "%s%s" % (self.config.get('amsl', 'uri-download-prefix'), holding[urikey])
                downloaded = shellout("curl --fail {link} > {output} ", link=link)
                try:
                    _ = zipfile.ZipFile(downloaded)
                    output = shellout("unzip -p {input} >> {output}", input=downloaded, output=stopover)
                except zipfile.BadZipfile:
                    # at least the file is not a zip.
                    output = shellout("cat {input} >> {output}", input=downloaded, output=stopover)

        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

class AMSLOpenAccessISSNList(AMSLTask):
    """
    List of ISSN, which are open access.

    For now, extract these ISSN from a special holding file, called
    KBART_FREEJOURNALS via AMSL.
    """

    date = luigi.Parameter(default=datetime.date.today())

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
            # at least the file is not a zip.
            output = shellout("cat {input} >> {output}", input=downloaded)

        shellout("cut -f 2 {input} | grep -oE '[0-9]{{4,4}}-[xX0-9]{{4,4}}' >> {output}", input=output, output=stopover)
        shellout("cut -f 3 {input} | grep -oE '[0-9]{{4,4}}-[xX0-9]{{4,4}}' >> {output}", input=output, output=stopover)
        output = shellout("sort -u {input} > {output}", input=stopover)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

class AMSLBuckets(AMSLTask):
    """
    Assemble attachment configuration from AMSL.

        {
          "DE-L229": {
            "48": {
              "holdings": [
                "https://x.y.z/get?setResource=Dokument/KBART_FREEJOURNALS",
                "https://x.y.z/get?setResource=Dokument/KBART_DEL229"
              ],
              "collections": [
                "Genios (Wirtschaftswissenschaften)",
                "Genios (Fachzeitschriften)"
              ]
            }
          },
          ...
    """
    date = luigi.Parameter(default=datetime.date.today())
    shard = luigi.Parameter(default='UBL-ai')

    def requires(self):
        return AMSLService(date=self.date, name='outboundservices:discovery')

    def run(self):
        with self.input().open() as handle:
            items = json.load(handle)

        tree = collections.defaultdict(dict)

        for item in items:
            if not item.get('shardLabel') == self.shard:
                continue

            isil, sid, cid = item.get('ISIL'), item.get('sourceID'), item.get('collectionLabel')

            if not sid in tree[isil]:
                tree[isil][sid] = collections.defaultdict(set)

            tree[isil][sid]['collections'].add(cid)

            if item.get('evaluateHoldingsFileForLibrary', False):
                if item.get('linkToHoldingsFile'):
                    tree[isil][sid]['holdings'].add(item.get('linkToHoldingsFile'))

            if item.get('externalLinkToContentFile', False):
                tree[isil][sid]['contents'].add(item.get('externalLinkToContentFile'))

            if item.get('linkToContentFile', False):
                tree[isil][sid]['contents'].add(item.get('linkToContentFile'))

        with self.output().open('w') as output:
            json.dump(tree, output, cls=SetEncoder)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='json'))

class AMSLFilterConfig(AMSLTask):
    """
    Convert to filterconfig format. With a few
    hard-wired exceptions for various sources.

        {
          "DE-L229": {
            "or": [
              {
                "or": [
                  {
                    "and": [
                      {
                        "source": [
                          "48"
                        ]
                      },
                      {
                        "package": [
                          "Genios (Wirtschaftswissenschaften)"
                        ]
                      },
                      {
                        "package": [
                          "Genios (Fachzeitschriften)"
                        ]
                      }
                    ]
                  },
        ...

    """
    date = luigi.Parameter(default=datetime.date.today())
    shard = luigi.Parameter(default='UBL-ai')

    def requires(self):
        return AMSLBuckets(date=self.date, shard=self.shard)

    def run(self):
        with self.input().open() as handle:
            items = json.load(handle)

        filterconfig = collections.defaultdict(dict)

        # konjuctions (or-terms) per ISIL
        konjs = collections.defaultdict(list)

        # the presence of this tag marks FZS / fulltext
        fzstag = 'Genios (Fachzeitschriften)'

        for isil, blob in items.iteritems():

            # DE-15-FID specific: only source ids and ISSN list
            if isil == 'DE-15-FID':
                filterconfig[isil] = {
                    'and': [
                        {'issn': {'url': self.config.get('amsl', 'fid-issn-list')}},
                        {'source': [sid for sid, _ in blob.items()]},
                    ]
                }
                continue

            for sid, filters in blob.iteritems():

                # exception: special treatment for genios
                if sid == "48":
                    # first, match packages FZS packages (FZS + X)
                    fzsterms = [{'source': [sid]}]
                    for name, c in filters.iteritems():
                        if name == 'collections':
                            if fzstag in c:
                                c.remove(fzstag)
                                fzsterms.append({'package': c})
                                fzsterms.append({'package': [fzstag]})

                    # then, match non FZS package, but also use holdings information
                    refterms = [{'source': [sid]}]
                    for name, c in filters.iteritems():
                        if name == 'holdings' or name == 'contents':
                            refterms.append({'holdings': {'urls': c}})
                        if name == 'collections':
                            if not fzstag in c:
                                refterms.append({'not': {'package': [fzstag]}})
                                refterms.append({'package': c})

                    konjs[isil].append({'or': [{"and": fzsterms}, {"and": refterms}]})
                    continue

                # exception: if we have jstor content files, then do not use collections
                if sid == "55":
                    terms = [{'source': [sid]}]
                    if 'contents' in filters:
                        for name, c in filters.iteritems():
                            if name == 'holdings' or name == 'contents':
                                terms.append({'holdings': {'urls': c}})
                    else:
                        for name, c in filters.iteritems():
                            if name == 'holdings' or name == 'contents':
                                terms.append({'holdings': {'urls': c}})
                            if name == 'collections':
                                terms.append({'collection': c})

                    konjs[isil].append({'and': terms})
                    continue

                # default handling
                terms = [{'source': [sid]}]

                for name, c in filters.iteritems():
                    if name in ('holdings', 'contents'):
                        terms.append({'holdings': {'urls': c}})
                    if name == 'collections':
                        terms.append({'collection': c})

                konjs[isil].append({'and': terms})

            filterconfig[isil] = {'or': konjs[isil]}

        with self.output().open('w') as output:
            json.dump(filterconfig, output)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='json'))
