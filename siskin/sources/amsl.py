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

"""
Electronic Resource Management System Based on Linked Data Technologies.

http://amsl.technology

Config:

[amsl]

uri-download-prefix = https://x.y.z/OntoWiki/files/get?setResource=
base = https://example.com

----

TODO:

Goal: Assemble complete attachment information (source and collection level,
holdings, lists and other filters) from AMSL API responses.

E.g. given a collection API response:

Filter relevant ISILs and extract source and collections

```
{
    "DE-X": {
        "and": [
            {
                "source": "123"
            },
            {
                "collection": ["...", "...", ...]
            }
        ]
    }
}
```

If there is an entry in AMSLHoldings for that ISIL, include holdings file.

```
{
    "DE-X": {
        "and": [
            {
                "source": "123"
            },
            {
                "collection": ["...", "...", ...]
            },
            {
                "holdings": {
                    "file": "..."
                }
            }
        ]
    }
}
```

If there is an entry in content files for a given collection name, use that (and
ignore / replace collection):

```
{
    "DE-D13": {
        "and": [
            {
                "source": "123"
            },
            {
                "holdings": {
                    "url": "url://to/content-file"
                }
            }
        ]
    }
}
```

"""

from gluish.format import TSV
from gluish.utils import shellout
from siskin.configuration import Config
from siskin.task import DefaultTask
from siskin.utils import SetEncoder
import collections
import datetime
import json
import luigi
import tempfile
import zipfile

config = Config.instance()

class AMSLTask(DefaultTask):
    TAG = 'amsl'

class AMSLService(AMSLTask):
    """
    Retrieve AMSL API response. Outbound: discovery, holdingsfiles, contentfiles, metadata_usage.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    name = luigi.Parameter(default='outboundservices:discovery')

    def run(self):
        parts = self.name.split(':')
        if not len(parts) == 2:
            raise RuntimeError('name must be of the form realm:name, e.g. outboundservices:discovery')
        realm, name = parts

        link = '%s/%s/list?do=%s' % (config.get('amsl', 'base').rstrip('/'), realm, name)
        output = shellout("""curl --fail "{link}" > {output} """, link=link)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True))

class AMSLCollectionsShardFilter(AMSLTask):
    """
    A per-shard list of collection entries.
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

    The output is probably zipped.

    One ISIL can have multiple files.

    If the file is Zipped, it will be decompressed.
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
        urikey = 'holdingsFileURI'

        for holding in holdings:
            if holding["ISIL"] == self.isil:

                if urikey not in holding:
                    raise RuntimeError('possible AMSL API change, expected: %s, available keys: %s' % (urikey, holding.keys()))

                # refs. #7142
                if 'kbart' not in holding[urikey].lower():
                    self.logger.debug("skipping non-KBART holding URI: %s" % holding[urikey])
                    continue

                link = "%s%s" % (config.get('amsl', 'uri-download-prefix'), holding[urikey])
                downloaded = shellout("curl --fail {link} > {output} ", link=link)
                try:
                    _ = zipfile.ZipFile(downloaded)
                    output = shellout("unzip -p {input} >> {output}", input=downloaded, output=stopover)
                except zipfile.BadZipfile:
                    # at least the file is not a zip.
                    output = shellout("cat {input} >> {output}", input=downloaded, output=stopover)

        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

class AMSLFilterConfig(AMSLTask):
    """
    Assemble attachment configuration from AMSL. WIP.
    """
    date = luigi.Parameter(default=datetime.date.today())
    shard = luigi.Parameter(default='UBL-ai')

    def requires(self):
        return AMSLService(date=self.date, name='outboundservices:discovery')

    def run(self):
        with self.input().open() as handle:
            items = json.load(handle)

        tree = collections.defaultdict(dict)
        # {
        #   "DE-L229": {
        #     "48": {
        #       "collections": [
        #         "Genios (Wirtschaftswissenschaften)",
        #         "Genios (Fachzeitschriften)"
        #       ],
        #       "uris": [
        #         "http://amsl.technology/discovery/metadata-usage/Dokument/KBART_FREEJOURNALS",
        #         "http://amsl.technology/discovery/metadata-usage/Dokument/KBART_DEL229"
        #       ]
        #     }
        #   },
        #   ...
        #   "DE-540": {
        #     "28": {
        #       "collections": [
        #         "DOAJ Directory of Open Access Journals"
        #       ]
        #     }
        #   },
        #   ...

        for item in items:
            if not item['shardLabel'] == self.shard:
                continue

            if not item['sourceID'] in tree[item['ISIL']]:
                tree[item['ISIL']][item['sourceID']] = {'collections': set()}
            tree[item['ISIL']][item['sourceID']]['collections'].add(item['collectionLabel'])

            if item['evaluateHoldingsFileForLibrary'] == 'yes' and item['holdingsFileURI'].strip():
                if not 'uris' in tree[item['ISIL']][item['sourceID']]:
                    tree[item['ISIL']][item['sourceID']]['uris'] = set()
                tree[item['ISIL']][item['sourceID']]['uris'].add(item['holdingsFileURI'])

        print(json.dumps(tree, cls=SetEncoder))

    def output(self):
        return luigi.LocalTarget(path=self.path())
