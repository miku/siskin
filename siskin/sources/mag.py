# coding: utf-8
# pylint: disable=C0301,E1101,W0622,C0103
#
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
# Microsoft Academic Graph
#
# TOS: http://research.microsoft.com/en-us/um/redmond/projects/mag/license.txt
#
# Links:
#
# * http://research.microsoft.com/en-us/projects/mag/
# * https://academicgraph.blob.core.windows.net/graph/index.html?eulaaccept=on
#
# Number of papers as of November 2015: 120887833, ~37M with DOI
#
# Schema
# Affiliations
#
#     Affiliation ID
#     Affiliation name
#
# Authors
#
#     Author ID
#     Author name
#
# Conference
#
#     Conference ID
#     Short name (abbreviation)
#     Full name
#
# ConferenceInstances
#
#     Conference ID
#     Conference instance ID
#     Short name (abbreviation)
#     Full name
#     Location
#     Official conference URL
#     Conference start date
#     Conference end date
#     Conference abstract registration date
#     Conference submission deadline date
#     Conference notification due date
#     Conference final version due date
#
# FieldsOfStudy
#
#     Field of study ID
#     Field of study name
#
# Journals
#
#     Journal ID
#     Journal name
#
# Papers
#
#     Paper ID
#     Original paper title
#     Normalized paper title
#     Paper publish year
#     Paper publish date
#     Paper Document Object Identifier (DOI)
#     Original venue name
#     Normalized venue name
#     Journal ID mapped to venue name
#     Conference series ID mapped to venue name
#     Paper rank
#
# PaperAuthorAffiliations
#
#     Paper ID
#     Author ID
#     Affiliation ID
#     Original affiliation name
#     Normalized affiliation name
#     Author sequence number
#
# PaperKeywords
#
#     Paper ID
#     Keyword name
#     Field of study ID mapped to keyword
#
# PaperReferences
#
#     Paper ID
#     Paper reference ID
#
# PaperUrls
#
#     Paper ID
#     URL

import datetime
import tempfile

import luigi
from gluish.format import Gzip
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.configuration import Config
from siskin.database import sqlitedb
from siskin.task import DefaultTask

config = Config.instance()


class MAGTask(DefaultTask):
    TAG = 'mag'

    def closest(self):
        return datetime.date(2016, 2, 5)


class MAGDates(MAGTask):
    """ Extract dates. """

    date = luigi.DateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("""
            curl "https://academicgraph.blob.core.windows.net/graph/index.html?eulaaccept=on" |
            grep "<li>" | grep -o "20[0-9][0-9]-[01][1-9]-[012][0-9]" | sort -u > {output} """)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())


class MAGDump(MAGTask):
    """ Dump file. """

    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("""
            curl https://academicgraph.blob.core.windows.net/graph-{date}/MicrosoftAcademicGraph.zip > {output}""",
                          date=self.closest())
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="zip"))


class MAGFile(MAGTask):
    """
    Listing archive: MicrosoftAcademicGraph.zip

    --
    Path = MicrosoftAcademicGraph.zip
    Type = zip
    64-bit = +
    Physical Size = 29698421155

       Date      Time    Attr         Size   Compressed  Name
    ------------------- ----- ------------ ------------  ------------------------
    2015-11-16 15:45:20 ....A       770185       296305  Affiliations.txt
    2015-11-16 15:48:48 ....A   3037120573   1472531330  Authors.txt
    2015-11-16 15:45:22 ....A     10346759      3502468  ConferenceInstances.txt
    2015-11-16 15:45:20 ....A        79774        26235  Conferences.txt
    2015-11-16 15:45:20 ....A      1508780       713334  FieldsOfStudy.txt
    2015-11-16 15:45:22 ....A      1002313       369790  Journals.txt
    2015-06-11 22:34:30 ....A         9641         3998  license.txt
    2015-11-16 16:03:48 ....A  18017900470   4298547230  PaperAuthorAffiliations.txt
    2015-11-16 15:51:32 ....A   5329643052   1839341847  PaperKeywords.txt
    2015-11-16 16:21:14 ....A  18094921016   7421585112  PaperReferences.txt
    2015-11-16 16:27:50 ....A  27419818455   8967796102  Papers.txt
    2015-11-16 16:22:22 ....A  27259393790   5693705196  PaperUrls.txt
    2015-08-30 17:21:24 ....A         1309          484  readme.txt
    ------------------- ----- ------------ ------------  ------------------------
                               99172516117  29698419431  13 files, 0 folders
    """
    date = ClosestDateParameter(default=datetime.date.today())
    name = luigi.Parameter(default='Papers')

    def requires(self):
        return MAGDump(date=self.date)

    def run(self):
        output = shellout("7z e -so {dump} {name}.txt | pigz -c > {output}", dump=self.input().path, name=self.name)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="txt.gz"), format=Gzip)


class MAGPaperDomains(MAGTask):
    """
    Domain distribution of Paper URLs.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return MAGFile(name='PaperUrls', date=self.date)

    def run(self):
        output = shellout("""
            unpigz -c {file} | LC_ALL=C grep -o "http[s]*://[^/]*" |
            LC_ALL=C sed -e 's@http[s]*://@@g' | TMPDIR={tmpdir} LC_ALL=C sort -S50% |
            uniq -c | sort -nr > {output} """,
                          tmpdir=config.get('core', 'tempdir'), file=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="txt.gz"))


class MAGDOIList(MAGTask):
    """
    List of DOI in this dataset.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return MAGFile(date=self.date, name='Papers')

    def run(self):
        output = shellout(
            """ unpigz -c {input} | LC_ALL=C cut -f6 | LC_ALL=C grep -v ^$ | LC_ALL=C sort -S50% | pigz -c > {output} """, input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="txt.gz"))


class MAGKeywordDistribution(MAGTask):
    """
    Keyword distribution.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return MAGFile(date=self.date, name='PaperKeywords')

    def run(self):
        output = shellout("unpigz -c {input} | cut -f2 | TMPDIR={tmpdir} sort -S50% | uniq -c | sort -nr | pigz -c > {output}",
                          input=self.input().path, tmpdir=config.get('core', 'tempdir'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="txt.gz"))


class MAGReferenceDB(MAGTask):
    """
    Create a sqlite3 version of references. Takes about 45min, database is about 26GB.

    Referenced DOIs in 10.1002/j.2168-9830.2008.tb00991.x:

        $ sqlite3 $(taskoutput MAGReferenceDB) "select doi from lookup where id IN (\
                                                    select ref from refs where id = (\
                                                        select id from lookup where doi = '10.1002/j.2168-9830.2008.tb00991.x'))"
        10.1109/FIE.2001.964026
        10.1002/j.2168-9830.1993.tb00065.x
        10.1002/j.2168-9830.2006.tb00883.x
        10.1002/j.2168-9830.1999.tb00449.x
        10.1007/s11948-007-9040-7
        10.1002/j.2168-9830.2005.tb00823.x
        10.1002/j.2168-9830.2005.tb00836.x
        10.1177/1466138104045659

    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'papers': MAGFile(name='Papers'),
            'refs': MAGFile(name='PaperReferences'),
        }

    def run(self):
        _, dbpath = tempfile.mkstemp(prefix='siskin-')

        # Create the lookup table from doi to internal id.
        with sqlitedb(dbpath) as cursor:
            query = cursor.execute("""CREATE TABLE IF NOT EXISTS lookup (doi text, id text)""")

        batch, size = [], 1000000
        with self.input().get('papers').open() as handle:
            for i, row in enumerate(handle):
                if i % size == 0:
                    self.logger.debug("[%d] adding batch (%d) ...", i, len(batch))
                    with sqlitedb(dbpath) as cursor:
                        query = cursor.executemany("""INSERT INTO lookup VALUES (?, ?)""", batch)
                    batch = []

                fields = row.strip().split('\t')
                id, doi = fields[0].strip(), fields[5].strip()
                if id == "" or doi == "":
                    continue

                batch.append((doi, id))

        self.logger.debug("creating index...")

        with sqlitedb(dbpath) as cursor:
            query = cursor.execute("""CREATE INDEX IF NOT EXISTS idx_lookup_doi ON lookup (doi)""")
            query = cursor.execute("""CREATE INDEX IF NOT EXISTS idx_lookup_id ON lookup (id)""")

        # Create the references table from internal id to internal id.
        with sqlitedb(dbpath) as cursor:
            query = cursor.execute("""CREATE TABLE IF NOT EXISTS refs (id text, ref text)""")

        batch, size = [], 1000000
        with self.input().get('refs').open() as handle:
            for i, row in enumerate(handle):
                if i % size == 0:
                    self.logger.debug("[%d] adding batch (%d) ...", i, len(batch))
                    with sqlitedb(dbpath) as cursor:
                        query = cursor.executemany("""INSERT INTO refs VALUES (?, ?)""", batch)
                    batch = []

                fields = row.strip().split('\t')
                id, ref = fields[0].strip(), fields[1].strip()
                if id == "" or ref == "":
                    continue

                batch.append((id, ref))

        self.logger.debug("creating index...")

        with sqlitedb(dbpath) as cursor:
            query = cursor.execute("""CREATE INDEX IF NOT EXISTS idx_refs_id ON refs (id)""")

        luigi.LocalTarget(dbpath).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="db"))
