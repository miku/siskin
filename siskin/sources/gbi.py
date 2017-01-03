# coding: utf-8
# pylint: disable=C0301,E1101

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

DEPRECATED. Newer, leaner workflow in module gbi.

--------

GBI publisher.

Workflow sketch:

Two kinds:

* Fulltext
* References

Two shipment types:

* Dump
* Update

Fulltext is called FZS.

TODO:

For fulltext, we take a Sigel-Kind-Package matrix and information about which
database belong to which package and calculate coverage from there.

For references, we reject all by default and only allow those items, which are
covered by a licence. Might change in the future.

Assets:

* gbi_package_db_map_fulltext.tsv
* gbi_package_db_map_refs.tsv
* gbi_package_sigel_map.json

Assumptions:

* A database is either reference or fulltext related, never both.

A few examples:

Database, that are existent in updates, but not dump:

    $ comm -23 <(taskcat GBIUpdateDatabaseList|sort) <(taskcat GBIDumpDatabaseList|cut -f3|sort)
    CHAN
    CIOD
    EXFO
    FLWA
    FSA
    PCW
    TEC
    WEN

Configuration:

[gbi]

scp-src = username@ftp.example.de:/home/gbi
rsync-options = -e XXX -avzP

"""

import collections
import datetime
import json
import os
import re
import tempfile

import luigi

from gluish.format import TSV
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.benchmark import timed
from siskin.common import Executable
from siskin.sources.amsl import AMSLFilterConfig
from siskin.task import DefaultTask
from siskin.utils import SetEncoder, iterfiles

# UPDATE and DUMP are addition configuration that might move into siskin.ini,
# once it is stable

UPDATES = {
    # Patterns for files containing updates. Change processing code too, if you
    # change this pattern, since it will depend on the capture group.
    "patterns": {
        "fulltext": re.compile(r'kons_sachs_fzs_([0-9]{14,14})_.*.zip'),
        "references": re.compile(r'kons_sachs_(?!fzs)[^_]+_([0-9]{14,14})_.*.zip'),
    }
}

# These files must be nested zips. If this changes, change the dependent tasks
# business logic as well.
DUMP = {
    "files": {
        "fulltext": [
            'mirror/gbi/FZS_NOV_2015.zip'
        ],
        "references": [
            'mirror/gbi/PSYN_NOV_2015.zip',
            'mirror/gbi/SOWI_NOV_2015.zip',
            'mirror/gbi/TECHN_NOV_2015.zip',
            'mirror/gbi/WIWI_NOV_2015.zip',
        ]
    },
    "date": datetime.date(2015, 11, 1),
}


class GBITask(DefaultTask):
    """
    GBI task.
    """
    TAG = '048'

    # DUMPTAG: the most recent update date as string.
    DUMPTAG = DUMP['date'].strftime("%Y%m%d%H%M%S")

    def closest(self):
        """ Update weekly. """
        return weekly(self.date)


class GBIDropbox(GBITask):
    """
    Pull down GBI dropbox content.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return Executable('rsync', message='https://rsync.samba.org/')

    def run(self):
        target = os.path.join(self.taskdir(), 'mirror')
        shellout("mkdir -p {target} && rsync {rsync_options} {src} {target}",
                 rsync_options=self.config.get(
                     'gbi', 'rsync-options', '-avzP'),
                 src=self.config.get('gbi', 'scp-src'), target=target)

        if not os.path.exists(self.taskdir()):
            os.makedirs(self.taskdir())

        with self.output().open('w') as output:
            for path in iterfiles(target):
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)


class GBIDump(GBITask):
    """
    List of GBI dump nested zips relative to the GBIDropbox taskdir.

    Note: Different for fulltext or references.
    """
    kind = luigi.Parameter(
        default='fulltext', description='fulltext or references')

    def requires(self):
        return GBIDropbox()

    def run(self):
        dropbox = GBIDropbox()
        with self.output().open('w') as output:
            if self.kind not in DUMP.get('files'):
                raise RuntimeError('no such fileset: %s, available: %s' % (
                    self.kind, DUMP.get('files')))
            files = DUMP['files'][self.kind]
            for path in files:
                abspath = os.path.join(dropbox.taskdir(), path)
                output.write_tsv(abspath)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)


class GBIDumpDatabaseList(GBITask):
    """
    A list of all database names in data dump, as seen they occur in the zipfiles.

    Note: Different for fulltext or references.
    """
    kind = luigi.Parameter(default='fulltext')

    def requires(self):
        return GBIDump(kind=self.kind)

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                shellout(r"""7z l {input} |
                             awk '{{print $6}}' |
                             grep "zip$" |
                             cut -d '.' -f1 |
                             awk '{{ print "{kind}\t{input}\t"$0 }}' >> {output}""", kind=self.kind, input=row.path, output=stopover)
        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class GBIDumpXML(GBITask):
    """
    Extract a single Database from a dump file.

    Note: DB can be fulltext or references.
    """
    issue = luigi.Parameter(
        default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    db = luigi.Parameter(description='name of the database to extract')

    def requires(self):
        return {'dblist-fulltext': GBIDumpDatabaseList(kind='fulltext'),
                'dblist-references': GBIDumpDatabaseList(kind='references'),
                '7z': Executable(name='7z', message='http://www.7-zip.org/')}

    def run(self):
        archive = None

        with self.input().get('dblist-fulltext').open() as handle:
            for row in handle.iter_tsv(cols=('kind', 'path', 'db')):
                if row.db == self.db:
                    archive = row.path

        with self.input().get('dblist-references').open() as handle:
            for row in handle.iter_tsv(cols=('kind', 'path', 'db')):
                if row.db == self.db:
                    archive = row.path

        if archive is None:
            # an non-existent database name amount to an empty dump file
            self.logger.debug('no such db: %s' % self.db)
            with self.output().open('w') as output:
                pass
            return

        dbzip = shellout(
            "7z x -so {archive} {db}.zip 2> /dev/null > {output}", archive=archive, db=self.db)
        output = shellout("""unzip -p {dbzip} \*.xml 2> /dev/null |
                             iconv -f iso-8859-1 -t utf-8 |
                             LC_ALL=C grep -v "^<\!DOCTYPE GENIOS PUBLIC" |
                             LC_ALL=C sed -e 's@<?xml version="1.0" encoding="ISO-8859-1" ?>@@g' |
                             LC_ALL=C sed -e 's@</Document>@<x-origin>{origin}</x-origin><x-issue>{issue}</x-issue></Document>@' |
                             pigz -c >> {output} """, dbzip=dbzip, origin=archive, issue=self.issue)

        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml.gz'))


class GBIDumpIntermediateSchema(GBITask):
    """
    Convert dump XML for DB into IS.
    """
    issue = luigi.Parameter(
        default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    db = luigi.Parameter(description='name of the database to extract')

    def requires(self):
        return {
            'file': GBIDumpXML(issue=self.issue, db=self.db),
            'span-import': Executable(name='span-import'),
            'pigz': Executable(name='pigz'),
        }

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout("span-import -i genios <(unpigz -c {input}) | pigz -c >> {output}",
                 input=self.input().get('file').path, output=stopover)
        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))


class GBIUpdateList(GBITask):
    """
    All GBI files that contain updates since a given date.

    Note: Different for fulltext or references.
    """
    since = luigi.DateParameter(
        default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())
    kind = luigi.Parameter(default='fulltext')

    def requires(self):
        return GBIDropbox(date=self.date)

    def run(self):
        since = self.since.strftime('%Y%m%d%H%M%S')

        with self.input().open() as handle:
            with self.output().open('w') as output:
                for row in sorted(handle.iter_tsv(cols=('path',))):
                    match = UPDATES['patterns'][self.kind].search(row.path)
                    if not match:
                        continue
                    filedate = match.group(1)
                    if filedate < since:
                        continue
                    output.write_tsv(row.path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)


class GBIUpdateDatabaseList(GBITask):
    """
    A list of databases contained in the updates. New databases might be added
    and only be visible in the updates.

    Note: Different for fulltext or references.
    """
    since = luigi.DateParameter(
        default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())
    kind = luigi.Parameter(default='fulltext')

    def requires(self):
        return GBIUpdateList(date=self.date, since=self.since, kind=self.kind)

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                shellout(""" unzip -p {input} | grep -o 'DB="[^"]*' | sed -e 's/DB="//g' >> {output} """,
                         input=row.path, output=stopover)
        output = shellout(
            "sort -u {input} > {output} && rm {input}", input=stopover)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class GBIDatabaseList(GBITask):
    """
    All database names, that occur, either in dump, update.

    Note: Different for fulltext or references.
    """
    since = luigi.DateParameter(
        default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())
    kind = luigi.Parameter(default='fulltext')

    def requires(self):
        return {'dump': GBIDumpDatabaseList(kind=self.kind),
                'update': GBIUpdateDatabaseList(date=self.date, since=self.since, kind=self.kind)}

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        output = shellout("cat <(cut -f3 {dump}) {update} | sort -u > {output}",
                          dump=self.input().get('dump').path, update=self.input().get('update').path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class GBIDatabaseType(GBITask):
    """
    Database-Type mapping generated directly from raw data.

    Contains info about all databases, from dumps, updates, references and fulltexts.
    """
    since = luigi.DateParameter(
        default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'fulltext': GBIDatabaseList(date=self.date, since=self.since, kind='fulltext'),
                'references': GBIDatabaseList(date=self.date, since=self.since, kind='references')}

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout(r"""cat {input} | awk '{{ print $0"\treferences"}}' >> {output}""",
                 input=self.input().get('references').path, output=stopover)
        shellout(r"""cat {input} | awk '{{ print $0"\tfulltext"}}' >> {output}""",
                 input=self.input().get('fulltext').path, output=stopover)
        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class GBIDatabaseMap(GBITask):
    """
    Create a map to use with span-import (assets/genios/dbmap.json).
    """
    since = luigi.DateParameter(
        default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GBIDatabaseType(date=self.date, since=self.since)

    def run(self):
        dbmap = collections.defaultdict(set)

        with luigi.LocalTarget(self.assets('gbi_package_db_map_fulltext.tsv'), format=TSV).open() as handle:
            for row in handle.iter_tsv(cols=('package', 'db')):
                dbmap[row.db].add(row.package)
                # Full text means Fachzeitschriften (FZS). FZS is a package as
                # well as an indicator to treat these records according to
                # license agreement.
                dbmap[row.db].add('Fachzeitschriften')

        with luigi.LocalTarget(self.assets('gbi_package_db_map_refs.tsv'), format=TSV).open() as handle:
            for row in handle.iter_tsv(cols=('package', 'db')):
                # Whereas reference databases are check along with a generic
                # holding file later.
                dbmap[row.db].add(row.package)

        with self.output().open('w') as output:
            output.write(json.dumps(dbmap, cls=SetEncoder) + "\n")

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class GBIUpdate(GBITask):
    """
    Concatenate updates.
    """
    since = luigi.DateParameter(
        default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())
    kind = luigi.Parameter(default='fulltext')

    def requires(self):
        return GBIUpdateList(date=self.date, kind=self.kind)

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in sorted(handle.iter_tsv(cols=('path',))):
                match = UPDATES['patterns'][self.kind].search(row.path)
                if not match:
                    continue
                filedate = match.group(1)
                shellout(""" unzip -p {input} | iconv -f iso-8859-1 -t utf-8 |
                             LC_ALL=C grep -v "^<\!DOCTYPE GENIOS PUBLIC" |
                             LC_ALL=C sed -e 's@<?xml version="1.0" encoding="ISO-8859-1" ?>@@g' |
                             LC_ALL=C sed -e 's@</Document>@<x-origin>{origin}</x-origin><x-group>TBA</x-group><x-issue>{issue}</x-issue></Document>@' >> {output}""",
                         input=row.path, origin=row.path, issue=filedate, output=stopover)

        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))


class GBIUpdateIntermediateSchema(GBITask):
    """
    Convert the combined updates to ischema.
    """
    since = luigi.DateParameter(
        default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return [GBIUpdate(since=self.since, date=self.date, kind='fulltext'),
                GBIUpdate(since=self.since, date=self.date, kind='references')]

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout(
                """span-import -i genios {input} | pigz -c >> {output}""", input=target.path, output=stopover)
        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))


class GBIUpdateIntermediateSchemaFiltered(GBITask):
    """
    Filter out items for a given database.
    """
    since = luigi.DateParameter(
        default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())
    db = luigi.Parameter(description='name of the database to extract')

    def requires(self):
        return GBIUpdateIntermediateSchema(since=self.since, date=self.date)

    def run(self):
        output = shellout("""jq -r -c '. | select(.["x.package"] == "{db}")' <(unpigz -c {input}) | pigz -c > {output}""",
                          db=self.db, input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))


class GBIDatabase(GBITask):
    """
    Combine dump and updates for a given database.
    """
    issue = luigi.Parameter(
        default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    since = luigi.DateParameter(
        default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())
    db = luigi.Parameter(description='name of the database to extract')

    def requires(self):
        return {
            'dump': GBIDumpIntermediateSchema(issue=self.issue, db=self.db),
            'update': GBIUpdateIntermediateSchemaFiltered(since=self.since, date=self.date, db=self.db)
        }

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for source, target in self.input().iteritems():
            shellout("cat {input} >> {output}",
                     input=target.path, output=stopover)

        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))


class GBIIntermediateSchemaByKind(GBITask):
    """
    Combine all references or fulltext databases into a single intermediate schema document.
    """
    issue = luigi.Parameter(
        default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    since = luigi.DateParameter(
        default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())
    kind = luigi.Parameter(default='fulltext')

    def requires(self):
        prereq = GBIDatabaseList(kind=self.kind)
        luigi.build([prereq])
        with prereq.output().open() as handle:
            for row in handle.iter_tsv(cols=('db',)):
                yield GBIDatabase(db=row.db, issue=self.issue, since=self.since, date=self.date)

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}",
                     input=target.path, output=stopover)
        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))


class GBIExportByKind(GBITask):
    """
    A SOLR-importable version of GBI, by kind.
    """
    issue = luigi.Parameter(
        default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    since = luigi.DateParameter(
        default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())
    kind = luigi.Parameter(default='fulltext')

    def requires(self):
        return {
            'file': GBIIntermediateSchemaByKind(issue=self.issue, since=self.since, date=self.date, kind=self.kind),
            'config': AMSLFilterConfig(date=self.date)
        }

    @timed
    def run(self):
        output = shellout("span-tag -c {config} <(unpigz -c {input}) | span-export | pigz -c > {output}",
                          input=self.input().get('file').path, config=self.input().get('config').path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))


class GBIDatabaseTable(GBITask):
    """
    For a given database name, extract the ID and date.
    """
    issue = luigi.Parameter(
        default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    since = luigi.DateParameter(
        default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())
    db = luigi.Parameter(description='name of the database to extract')

    def requires(self):
        return GBIDatabase(issue=self.issue, since=self.since, date=self.date, db=self.db)

    def run(self):
        output = shellout(
            r"""jq -r '[.["finc.record_id"], .["x.indicator"]] | @csv' <(unpigz -c {input}) | tr -d '"' | tr ',' '\t' > {output}""", input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class GBIISSNDatabase(GBITask):
    """
    Record ISSN per record per database.
    """
    issue = luigi.Parameter(
        default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    since = luigi.DateParameter(
        default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())
    db = luigi.Parameter(description='name of the database to extract')

    def requires(self):
        return GBIDatabase(issue=self.issue, since=self.since, date=self.date, db=self.db)

    def run(self):
        output = shellout(r"""jq -r '[.["finc.record_id"], .["rft.issn"][]? // "NOT_AVAILABLE"] | @csv' <(unpigz -c {input}) |
                              tr -d '"' | tr ',' '\t' | awk '{{ print "{db}\t"$0 }}'> {output}""", input=self.input().path, db=self.db)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class GBIPublicationTitleDatabase(GBITask):
    """
    Extract publication title per database.
    """
    issue = luigi.Parameter(
        default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    since = luigi.DateParameter(
        default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())
    db = luigi.Parameter(description='name of the database to extract')

    def requires(self):
        return GBIDatabase(issue=self.issue, since=self.since, date=self.date, db=self.db)

    def run(self):
        output = shellout(r"""jq -r '[.["finc.record_id"], .["rft.jtitle"]? // "NOT_AVAILABLE"] | @csv' <(unpigz -c {input}) |
                              tr -d '"' | tr ',' '\t' | awk '{{ print "{db}\t"$0 }}' > {output}""", input=self.input().path, db=self.db)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class GBIPublicationTitleWrapper(GBITask, luigi.WrapperTask):
    """
    Just a wrapper to prepare publication title lists.
    """
    issue = luigi.Parameter(
        default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    since = luigi.DateParameter(
        default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())
    kind = luigi.Parameter(default='fulltext')

    def requires(self):
        prerequisite = GBIDatabaseList(kind=self.kind)
        luigi.build([prerequisite])
        with prerequisite.output().open() as handle:
            for row in handle.iter_tsv(cols=('db',)):
                yield GBIPublicationTitleDatabase(issue=self.issue, since=self.since, date=self.date, db=row.db)

    def output(self):
        return self.input()


class GBIPublicationTitleOverview(GBITask):
    """
    Just combine all publication titles.
    """
    issue = luigi.Parameter(
        default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    since = luigi.DateParameter(
        default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'references': GBIPublicationTitleWrapper(issue=self.issue, since=self.since, date=self.date, kind='references'),
                'fulltext': GBIPublicationTitleWrapper(issue=self.issue, since=self.since, date=self.date, kind='fulltext')}

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for kind, targets in self.input().iteritems():
            for target in targets:
                shellout(""" cat {input} | awk '{{ print "{kind}\t"$0 }}' >> {output} """,
                         input=target.path, output=stopover, kind=kind)
        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class GBITitleDatabase(GBITask):
    """
    Extract title per database, also ISSN.
    """
    issue = luigi.Parameter(
        default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    since = luigi.DateParameter(
        default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())
    db = luigi.Parameter(description='name of the database to extract')

    def requires(self):
        return GBIDatabase(issue=self.issue, since=self.since, date=self.date, db=self.db)

    def run(self):
        output = shellout(r"""jq -r '[.["finc.record_id"], .["rft.issn"][]? // "NOT_AVAILABLE", .["rft.atitle"]? // "NOT_AVAILABLE"] | @csv' <(unpigz -c {input}) |
                              tr -d '"' | tr ',' '\t' | awk '{{ print "{db}\t"$0 }}' > {output}""", input=self.input().path, db=self.db)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class GBITitleWrapper(GBITask, luigi.WrapperTask):
    """
    Just a wrapper to prepare title lists.

    Note: fulltext or reference.
    """
    issue = luigi.Parameter(
        default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    since = luigi.DateParameter(
        default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())
    kind = luigi.Parameter(default='fulltext')

    def requires(self):
        prerequisite = GBIDatabaseList(kind=self.kind)
        luigi.build([prerequisite])
        with prerequisite.output().open() as handle:
            for row in handle.iter_tsv(cols=('db',)):
                yield GBITitleDatabase(issue=self.issue, since=self.since, date=self.date, db=row.db)

    def output(self):
        return self.input()


class GBITitleList(GBITask):
    """
    Just a wrapper to prepare title lists.

    Note: fulltext or reference.
    """
    issue = luigi.Parameter(
        default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    since = luigi.DateParameter(
        default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())
    kind = luigi.Parameter(default='fulltext')

    def requires(self):
        return GBITitleWrapper(issue=self.issue, since=self.since, date=self.date, kind=self.kind)

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}",
                     input=target.path, output=stopover)
        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class GBIDatabaseISSNWrapper(GBITask, luigi.WrapperTask):
    """
    Just a wrapper to prepare ISSN lists.
    """
    issue = luigi.Parameter(
        default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    since = luigi.DateParameter(
        default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())
    kind = luigi.Parameter(default='fulltext')

    def requires(self):
        prerequisite = GBIDatabaseList(kind=self.kind)
        luigi.build([prerequisite])
        with prerequisite.output().open() as handle:
            for row in handle.iter_tsv(cols=('db',)):
                yield GBIISSNDatabase(issue=self.issue, since=self.since, date=self.date, db=row.db)

    def output(self):
        return self.input()


class GBIISSNList(GBITask):
    """
    A list of ISSN in GBI. About 400.
    """
    issue = luigi.Parameter(
        default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    since = luigi.DateParameter(
        default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())
    kind = luigi.Parameter(default='fulltext')

    def requires(self):
        return GBIDatabaseISSNWrapper(issue=self.issue, since=self.since, date=self.date, kind=self.kind)

    def run(self):
        issns = set()
        for target in self.input():
            with target.open() as handle:
                for line in handle:
                    parts = line.strip().split('\t')
                    if len(parts) < 3:
                        continue
                    match = re.search(
                        '.*([0-9]{4}-[0-9]{3}[0-9X]).*', parts[2])
                    if match:
                        issns.add(match.group(1))

        with self.output().open('w') as output:
            for issn in sorted(issns):
                output.write_tsv(issn)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class GBIDatabaseWrapper(GBITask, luigi.WrapperTask):
    """
    Just a wrapper to prepare all databases.
    """
    issue = luigi.Parameter(
        default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    since = luigi.DateParameter(
        default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())
    kind = luigi.Parameter(default='fulltext')

    def requires(self):
        prerequisite = GBIDatabaseList(kind=self.kind)
        luigi.build([prerequisite])
        with prerequisite.output().open() as handle:
            for row in handle.iter_tsv(cols=('db',)):
                yield GBIDatabase(issue=self.issue, since=self.since, date=self.date, db=row.db)

    def output(self):
        return self.input()


class GBIISSNStats(GBITask):
    """
    For each database list the percentage of records that have an ISSN.
    """
    issue = luigi.Parameter(
        default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    since = luigi.DateParameter(
        default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())
    kind = luigi.Parameter(default='fulltext')

    def requires(self):
        return GBIDatabaseISSNWrapper(issue=self.issue, since=self.since, date=self.date, kind=self.kind)

    def run(self):
        issn_pattern = re.compile(
            r'[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9xX]')
        with self.output().open('w') as output:
            for target in self.input():
                stats = collections.defaultdict(int)
                issns = set()
                with target.open() as handle:
                    try:
                        for row in handle.iter_tsv():
                            if len(row) < 3:
                                stats['skipped'] += 1
                                continue
                            db, id, issn = row[0], row[1], row[2]

                            stats['db'] = db
                            stats['total'] += 1
                            if issn == 'NOT_AVAILABLE':
                                stats['noissn'] += 1
                            else:
                                stats['issn'] += 1
                                issns.add(issn)
                                if issn_pattern.search(issn):
                                    stats['valid'] += 1
                                else:
                                    stats['invalid'] += 1

                        stats['issncount'] = len(issns)
                        if stats['total'] > 0:
                            stats['withissn_percentage'] = '%0.2f' % (
                                100.0 / stats['total'] * stats['issn'])
                        else:
                            stats['withissn_percentage'] = 0
                        output.write_tsv(stats['db'], 'ok', stats['total'], stats['withissn_percentage'], stats['issncount'],
                                         stats['issn'], stats['noissn'], stats['valid'], stats['invalid'], stats['skipped'])
                    except Exception as err:
                        output.write_tsv(
                            stats['db'], 'err', 0, 0, 0, 0, 0, 0, 0, 0)
                        self.logger.debug(err)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
