# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301

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
[jstor]

ftp-host = host.name
ftp-username = username
ftp-password = password
ftp-path = /
ftp-pattern = *
"""

import collections
import datetime
import itertools
import pipes
import tempfile

import luigi
import ujson as json
from gluish.format import TSV, Gzip
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.benchmark import timed
from siskin.common import Executable, FTPMirror
from siskin.sources.amsl import AMSLFilterConfig, AMSLService
from siskin.task import DefaultTask
from siskin.utils import SetEncoder, nwise, load_set_from_file


class JstorTask(DefaultTask):
    """ Jstor base. """
    TAG = 'jstor'

    def closest(self):
        return weekly(self.date)


class JstorPaths(JstorTask):
    """
    Sync.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    max_retries = luigi.IntParameter(default=10, significant=False)
    timeout = luigi.IntParameter(
        default=20, significant=False, description='timeout in seconds')

    def requires(self):
        return FTPMirror(host=self.config.get('jstor', 'ftp-host'),
                         username=self.config.get('jstor', 'ftp-username'),
                         password=self.config.get('jstor', 'ftp-password'),
                         pattern=self.config.get('jstor', 'ftp-pattern'),
                         max_retries=self.max_retries,
                         timeout=self.timeout)

    def run(self):
        self.input().move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class JstorMembers(JstorTask):
    """
    Extract a full list of archive members.
    TODO: This should only be done once per file.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return JstorPaths(date=self.date)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                if not row.path.endswith('.zip'):
                    self.logger.debug('skipping: %s', row.path)
                    continue
                shellout(""" unzip -l {input} | LC_ALL=C grep "xml$" | LC_ALL=C awk '{{print "{input}\t"$4}}' | LC_ALL=C sort >> {output} """,
                         preserve_whitespace=True, input=row.path, output=stopover)
        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class JstorLatestMembers(JstorTask):
    """
    Find the latest archive members for all article ids.

    The path names are like XXX_YYYY-MM-DD-HH-MM-SS/jjjj/yyyy-mm-dd/id/id.xml.

    This way, it is possible to sort the shipments by date, run `tac` and
    only keep the latest entries.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return JstorMembers(date=self.date)

    @timed
    def run(self):
        """
        Expect input to be sorted by shipment date, so tac will actually be a perfect rewind.
        """
        output = shellout("tac {input} | LC_ALL=C sort -S 35% -u -k2,2 | LC_ALL=C sort -S 35% -k1,1 > {output}",
                          input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class JstorXML(JstorTask):
    """
    Create a snapshot of the latest data.
    TODO(miku): maybe shard by journal and reduce update time.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    batch = luigi.IntParameter(default=512, significant=False)

    def requires(self):
        return JstorLatestMembers(date=self.date)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            groups = itertools.groupby(handle.iter_tsv(
                cols=('archive', 'member')), lambda row: row.archive)
            for archive, items in groups:
                for chunk in nwise(items, n=self.batch):
                    margs = " ".join(["'%s'" % item.member.decode(
                        encoding='utf-8').replace('[', r'\[').replace(']', r'\]') for item in chunk])
                    shellout("""unzip -p {archive} {members} |
                                sed -e 's@<?xml version="1.0" encoding="UTF-8"?>@@g' | pigz -c >> {output}""",
                             archive=archive.decode(encoding='utf-8'), members=margs, output=stopover)

        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml.gz'), format=TSV)


class JstorIntermediateSchemaGenericCollection(JstorTask):
    """
    Convert to intermediate format via span. Use generic JSTOR collection name.
    """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'span': Executable(name='span-import', message='http://git.io/vI8NV'),
            'file': JstorXML(date=self.date)
        }

    @timed
    def run(self):
        output = shellout("span-import -i jstor <(unpigz -c {input}) | pigz -c > {output}",
                          input=self.input().get('file').path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)


class JstorCollectionMapping(JstorTask):
    """
    Create a mapping from ISSN to collection name.

    Experimental, refs #11467. See: http://www.jstor.org/kbart/collections/all-archive-titles

        {
        "1957-7745": [
            "Arts & Sciences XIV Collection"
        ],
        "2159-4538": [
            "Arts & Sciences XII Collection"
        ],
        "0272-5045": [
            "Law Discipline Package",
            "American Society of International Law Package",
            "Arts & Sciences VI Collection"
        ],
        ...
    """
    date = ClosestDateParameter(default=datetime.date.today())

    @timed
    def run(self):
        names = collections.defaultdict(set)
        url = "http://www.jstor.org/kbart/collections/all-archive-titles"
        output = shellout("""curl -sL "{url}" > {output} """, url=url)

        with luigi.LocalTarget(output, format=TSV).open() as handle:
            for line in handle.iter_tsv():
                issns, parts = line[1:3], [p.strip()
                                           for p in line[26].split(";")]
                for issn in [v.strip() for v in issns]:
                    if not issn:
                        continue
                    for name in parts:
                        if not name:
                            continue
                        names[issn].add(name)

        with self.output().open('w') as output:
            import json  # ujson does not support cls keyword
            json.dump(names, output, cls=SetEncoder)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='json'))


class JstorIntermediateSchema(JstorTask):
    """
    Turn single collection name "JStor" (https://git.io/vdHYh) into finer
    grained names via title lists (https://is.gd/W37Uwg), refs #11467.

    @2017-01-23 JSTOR ISSN to Collection mapping: https://git.io/vNwSJ
    @2017-01-23 AMSL:
        JSTOR Arts & Sciences I Archive
        JSTOR Arts & Sciences II Archive
        JSTOR Arts & Sciences III Archive
        JSTOR Arts & Sciences IV Archive
        JSTOR Arts & Sciences IX Archive
        JSTOR Arts & Sciences V Archive
        JSTOR Arts & Sciences VI Archive
        JSTOR Arts & Sciences VII Archive
        JSTOR Arts & Sciences VIII Archive
        JSTOR Arts & Sciences X Archive
        JSTOR Arts & Sciences XI Archive
        JSTOR Arts & Sciences XII Archive
        JSTOR Arts & Sciences XIII Archive
        JSTOR Arts & Sciences XIV Archive
        JSTOR Arts & Sciences XV Archive
        JSTOR Business I Archive
        JSTOR Business II Archive
        JSTOR Film and Performing Arts
        JSTOR Language & Literature Archive
        JSTOR Life Sciences Archive
        JSTOR Music Archive

    XXX: Why is JSTOR (about 500 records) still attached for DE-15, refs #12066.
    """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'file': JstorIntermediateSchemaGenericCollection(date=self.date),
            'mapping': JstorCollectionMapping(date=self.date),
            'amsl': AMSLService(date=self.date),
        }

    @timed
    def run(self):
        """
        Only use collection names, which we find in AMSL as well. XXX: FIX deviations.
        """
        output = shellout("""cat {input} | jq -rc '.[] | select(.sourceID == "55") | .megaCollection' > {output} """,
                          input=self.input().get("amsl").path)
        allowed_collection_names = load_set_from_file(output)
        self.logger.debug("allowing via AMSL: %s", allowed_collection_names)

        # XXX: Hack to map original JSTOR collection names to names in AMSL. We
        # will need an authoritative source of such names.
        jstor_amsl_collection_name_mapping = {
            'Arts & Sciences I Collection': 'JSTOR Arts & Sciences I Archive',
            'Arts & Sciences II Collection': 'JSTOR Arts & Sciences II Archive',
            'Arts & Sciences III Collection': 'JSTOR Arts & Sciences III Archive',
            'Arts & Sciences IV Collection': 'JSTOR Arts & Sciences IV Archive',
            'Arts & Sciences IX Collection': 'JSTOR Arts & Sciences IX Archive',
            'Arts & Sciences V Collection': 'JSTOR Arts & Sciences V Archive',
            'Arts & Sciences VI Collection': 'JSTOR Arts & Sciences VI Archive',
            'Arts & Sciences VII Collection': 'JSTOR Arts & Sciences VII Archive',
            'Arts & Sciences VIII Collection': 'JSTOR Arts & Sciences VIII Archive',
            'Arts & Sciences X Collection': 'JSTOR Arts & Sciences X Archive',
            'Arts & Sciences XI Collection': 'JSTOR Arts & Sciences XI Archive',
            'Arts & Sciences XII Collection': 'JSTOR Arts & Sciences XII Archive',
            'Arts & Sciences XIII Collection': 'JSTOR Arts & Sciences XIII Archive',
            'Arts & Sciences XIV Collection': 'JSTOR Arts & Sciences XIV Archive',
            'Arts & Sciences XV Collection': 'JSTOR Arts & Sciences XV Archive',
            'Business I Collection': 'JSTOR Business I Archive',
            'Business II Collection': 'JSTOR Business II Archive',
            'Film & Performing Arts Discipline Package': 'JSTOR Film and Performing Arts',
            'Language & Literature Discipline Package': 'JSTOR Language & Literature Archive',
            'Life Sciences Collection': 'JSTOR Life Sciences Archive',
            'Music Collection': 'JSTOR Music Archive',
        }

        with self.input().get('mapping').open() as mapfile:
            mapping = json.load(mapfile)

        counter = collections.Counter()

        with self.input().get('file').open() as handle:
            with self.output().open('w') as output:
                for line in handle:
                    doc = json.loads(line)
                    issns, names = set(), set()

                    for issn in doc.get('rft.issn', []):
                        issns.add(issn)
                    for issn in doc.get('rft.eissn', []):
                        issns.add(issn)

                    for issn in issns:
                        for name in mapping.get(issn, []):
                            names.add(name)

                    if len(names) > 0:
                        # Translate JSTOR names to AMSL.
                        amsl_names = [jstor_amsl_collection_name_mapping.get(name) for name in names if name in jstor_amsl_collection_name_mapping]
                        # Check validity against AMSL names.
                        clean_names = [name for name in amsl_names if name in allowed_collection_names]

                        # Use names that appear in AMSL.
                        doc['finc.mega_collection'] = clean_names

                        if len(doc['finc.mega_collection']) == 0:
                            self.logger.warn("no collection name given to %s: %s", doc["finc.id"], names)
                            counter["err.collection.not.in.amsl"] += 1
                    else:
                        self.logger.warn("JSTOR record without issn or issn mapping: %s", doc.get("finc.id"))
                        counter["err.name"] += 1

                    json.dump(doc, output)
                    output.write("\n")

        self.logger.debug(counter)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)


class JstorExport(JstorTask):
    """
    Tag with ISILs, then export to various formats.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='solr5vu3', description='export format')

    def requires(self):
        return {
            'file': JstorIntermediateSchema(date=self.date),
            'config': AMSLFilterConfig(date=self.date),
        }

    def run(self):
        output = shellout("span-tag -c {config} <(unpigz -c {input}) | pigz -c > {output}",
                          config=self.input().get('config').path, input=self.input().get('file').path)
        output = shellout(
            "span-export -o {format} <(unpigz -c {input}) | pigz -c > {output}", format=self.format, input=output)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        extensions = {
            'solr5vu3': 'ldj.gz',
            'formeta': 'form.gz',
        }
        return luigi.LocalTarget(path=self.path(ext=extensions.get(self.format, 'gz')))


class JstorISSNList(JstorTask):
    """
    A list of JSTOR ISSNs.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return JstorIntermediateSchema(date=self.date)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout(
            """jq -r '.["rft.issn"][]?' <(unpigz -c {input}) 2> /dev/null >> {output} """, input=self.input().path, output=stopover)
        shellout(
            """jq -r '.["rft.eissn"][]?' <(unpigz -c {input}) 2> /dev/null >> {output} """, input=self.input().path, output=stopover)
        output = shellout("""sort -u {input} > {output} """, input=stopover)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class JstorDOIList(JstorTask):
    """
    A list of JSTOR DOIs.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return JstorIntermediateSchema(date=self.date)

    @timed
    def run(self):
        output = shellout("""jq -r '.doi' <(unpigz -c {input}) | grep -v null > {output} """,
                          input=self.input().path)
        output = shellout("""sort -u {input} > {output} """, input=output)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
