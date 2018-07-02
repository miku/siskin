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
import os
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
    TAG = '55'

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

    2018-03-10: There is a slight change in format, refs #12669.

    Previously, update would ship in folders starting with MMS:

        MMS_2015-03-23-18-04-17/...

    with each folder containing a couple of ZIP files containing a couple of XML files.

        MMS_2015-03-23-18-04-17/ocealing_20170904T133503_1.zip

    These XML files are listed in the second column of this tasks' output.

        $ unzip -l MMS_2017-09-04-13-31-15/ocealing_20170904T133503_1.zip
        Archive:  MMS_2017-09-04-13-31-15/ocealing_20170904T133503_1.zip
        Length      Date    Time    Name
        ---------  ---------- -----   ----
            3053  2017-09-04 13:35   ocealing/2002-06-01/3623336/3623336.xml
            3392  2017-09-04 13:35   ocealing/2002-06-01/3623338/3623338.xml
            4960  2017-09-04 13:35   ocealing/2002-06-01/3623326/3623326.xml
            3002  2017-09-04 13:35   ocealing/2002-06-01/3623335/3623335.xml
            4815  2017-09-04 13:35   ocealing/2002-06-01/3623327/3623327.xml
            3980  2017-09-04 13:35   ocealing/2002-06-01/3623330/3623330.xml
            2463  2017-09-04 13:35   ocealing/2002-06-01/3623339/3623339.xml
            2814  2017-09-04 13:35   ocealing/2002-06-01/3623325/3623325.xml
            2425  2017-09-04 13:35   ocealing/2002-06-01/3623340/3623340.xml
            3170  2017-09-04 13:35   ocealing/2002-06-01/3623337/3623337.xml
            3973  2017-09-04 13:35   ocealing/2002-06-01/3623329/3623329.xml
            3874  2017-09-04 13:35   ocealing/2002-06-01/3623332/3623332.xml
            3051  2017-09-04 13:35   ocealing/2002-06-01/3623334/3623334.xml
            3536  2017-09-04 13:35   ocealing/2002-06-01/3623333/3623333.xml
            2423  2017-09-04 13:35   ocealing/2002-06-01/3623324/3623324.xml
            3600  2017-09-04 13:35   ocealing/2002-06-01/3623331/3623331.xml
            4254  2017-09-04 13:35   ocealing/2002-06-01/3623328/3623328.xml
        ---------                     -------
            58785                     17 files

    As of 2018-03-13 there are 30 new zip files in the root folder, 29 of which follow the naming scheme:

        jstor-journals-2018-03-10T06-52-44Z-part-[0-9]{3,3}.zip

    And one:

        jstor-journals-csp-2018-03-10T17-46-45Z-part-001.zip

    This tasks picked up all the files, which is why the number of records increased:

        $ taskcat JstorLatestMembers --date 2018-03-05 | wc -l
        10290925

        $ taskcat JstorLatestMembers --date 2018-03-13 | wc -l
        22136409

    One simple way to distinguish these sets:

        $ taskcat JstorLatestMembers --date 2018-03-05 | grep -c metadata
        0

        $ taskcat JstorLatestMembers --date 2018-03-13 | grep -c metadata
        11845484

        $ echo 22136409-10290925 | bc -l
        11845484

    We assume, this is a complete set of the content, so we can choose either the one or the other.

    Assumed update handling. As of 2018-07-01, there are five updates files:

        jstor-journals-updates-csp-2018-06-28T20-35-03Z-part-001.zip
        jstor-journals-updates-csp-2018-06-29T13-06-49Z-part-001.zip
        jstor-journals-updates-archives-2018-06-28T19-38-43Z-part-001.zip
        jstor-journals-updates-archives-2018-06-28T19-38-43Z-part-002.zip
        jstor-journals-updates-archives-2018-06-29T13-07-02Z-part-001.zip
        jstor-journals-updates-archives-2018-06-30T13-00-03Z-part-001.zip

    These files range in size from a few kB to almost a GB.

    XXX: Adjust snapshotting accordingly.

    Issue, refs #12669.
    """

    date = ClosestDateParameter(default=datetime.date.today())
    version = luigi.IntParameter(default=2, description="#12669")

    def requires(self):
        return JstorMembers(date=self.date)

    @timed
    def run(self):
        """
        Expect input to be sorted by shipment date, so tac will actually be a perfect rewind.
        """
        if self.version not in (1, 2):
            raise ValueError("supported versions: 1, 2 (refs #12669)")

        if self.version == 1:
            output = shellout("tac {input} | grep -v metadata | LC_ALL=C sort -S 35% -u -k2,2 | LC_ALL=C sort -S 35% -k1,1 > {output}",
                              input=self.input().path)
        if self.version == 2:
            output = shellout("tac {input} | grep metadata | LC_ALL=C sort -S 35% -u -k2,2 | LC_ALL=C sort -S 35% -k1,1 > {output}",
                              input=self.input().path)

        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class JstorXML(JstorTask):
    """
    Create a snapshot of the latest data. Using unzippa[1] to speed up
    extraction.

    [1] https://github.com/miku/unzippa
    """
    date = ClosestDateParameter(default=datetime.date.today())
    version = luigi.IntParameter(default=2, description="#12669")

    def requires(self):
        return JstorLatestMembers(date=self.date, version=self.version)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            groups = itertools.groupby(handle.iter_tsv(
                cols=('archive', 'member')), lambda row: row.archive)

            for archive, items in groups:
                # Write members to extract to temporary file.
                _, memberfile = tempfile.mkstemp(prefix='siskin-')
                with open(memberfile, 'w') as output:
                    for item in items:
                        output.write("%s\n" % item.member)

                self.logger.debug("for archive %s extract via: %s", archive, memberfile)

                # The unzippa will not exhaust ARG_MAX.
                shellout("""unzippa -v -m {memberfile} {archive} |
                            sed -e 's@<?xml version="1.0" encoding="UTF-8"?>@@g' | pigz -c >> {output}""",
                         archive=archive.decode(encoding='utf-8'), memberfile=memberfile, output=stopover)

                try:
                    os.remove(output.name)
                except OSError as err:
                    self.logger.warn(err)

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

    The KBART fields are:

     0 publication_title                                                                                                                                                                                              │·
     1 print_identifier                                                                                                                                                                                               │·
     2 online_identifier                                                                                                                                                                                              │·
     3 date_first_issue_online                                                                                                                                                                                        │·
     4 num_first_vol_online                                                                                                                                                                                           │·
     5 num_first_issue_online                                                                                                                                                                                         │·
     6 date_last_issue_online                                                                                                                                                                                         │·
     7 num_last_vol_online                                                                                                                                                                                            │·
     8 num_last_issue_online                                                                                                                                                                                          │·
     9 title_url                                                                                                                                                                                                      │·
    10 first_author                                                                                                                                                                                                   │·
    11 title_id                                                                                                                                                                                                       │·
    12 embargo_info                                                                                                                                                                                                   │·
    13 coverage_depth                                                                                                                                                                                                 │·
    14 notes                                                                                                                                                                                                          │·
    15 publisher_name                                                                                                                                                                                                 │·
    16 publication_type                                                                                                                                                                                               │·
    17 date_monograph_published_print                                                                                                                                                                                 │·
    18 date_monograph_published_online                                                                                                                                                                                │·
    19 monograph_volume                                                                                                                                                                                               │·
    20 monograph_edition                                                                                                                                                                                              │·
    21 first_editor                                                                                                                                                                                                   │·
    22 parent_publication_title_id                                                                                                                                                                                    │·
    23 preceding_publication_title_id                                                                                                                                                                                 │·
    24 access_type                                                                                                                                                                                                    │·
    25 full_coverage                                                                                                                                                                                                  │·
    26 collection                                                                                                                                                                                                     │·
    27 discipline                                                                                                                                                                                                     │·
    28 catalog_identifier_oclc                                                                                                                                                                                        │·
    29 catalog_identifier_lccn                                                                                                                                                                                        │·
    30 archive_release_date                                                                                                                                                                                           │·
    31 publication_date

    """
    date = ClosestDateParameter(default=datetime.date.today())

    @timed
    def run(self):
        names = collections.defaultdict(set)
        url = "http://www.jstor.org/kbart/collections/all-archive-titles"
        output = shellout("""curl -sL "{url}" > {output} """, url=url)

        with luigi.LocalTarget(output, format=TSV).open() as handle:
            for row in handle.iter_tsv():
                if len(row) < 27:
                    self.logger.warn("short KBART row, skipping: %s", row)
                    continue

                issns, parts = row[1:3], [p.strip()
                                           for p in row[26].split(";")]
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
