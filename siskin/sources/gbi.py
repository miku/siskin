#!/usr/bin/env python
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

"""
GBI publisher.

Workflow sketch:

Two variants.

* Fulltext
* References

Metadata w/ fulltext is bundled FZS, references the others.

For fulltext, we take a Sigel-Package matrix and information about which
database belong to which package and calculate coverage from there.

For references, we reject all by default and only allow those items, which are
covered by a licence.

To create an intermediate schema for fulltext of reference items, use the `kind` parameter:

    $ taskdo GBIIntermediateSchema --kind fulltext

The matrix is hand-compiled from data made available by the publisher.

TODO(miku):

Try to segment data by database, so updates can be calculated much quicker.

Example: Extract and keep zipfiles of DBs. For each DB convert to IS. Quicker
for other stats per database as well.

Assumptions: A database is either reference or fulltext related, never both.

[gbi]

scp-src = username@ftp.example.de:/home/gbi

"""

from gluish.format import TSV, Gzip
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.benchmark import timed
from siskin.common import FTPMirror, Executable
from siskin.configuration import Config
from siskin.task import DefaultTask
from siskin.utils import iterfiles
import collections
import datetime
import json
import luigi
import os
import re
import shutil
import tempfile
import zipfile

config = Config.instance()

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
    TAG = '048'
    DUMPTAG = DUMP['date'].strftime("%Y%m%d%H%M%S")

    def closest(self):
        """ Update weekly. """
        return weekly(self.date)

    def group(self, filename):
        """
        Given a filename, like `Recht-Law_NOV_2015.zip` return a canonical group name.
        The group name is then resolved to a collection during [span-import](http://git.io/vEMXR).
        """
        if re.search('recht', filename, re.IGNORECASE):
            return 'recht'
        if re.search('SOWI', filename):
            return 'sowi'
        if re.search('TECHN', filename):
            return 'technik'
        if re.search('FZS', filename, re.IGNORECASE):
            return 'fzs'
        if re.search('WIWI', filename):
            return 'wiwi'
        if re.search('PSYN', filename):
            return 'psyn'
        if re.search('_lit_', filename):
            return 'lit'
        raise RuntimeError('cannot determine group attribute from: %s' % filename)

class GBIDropbox(GBITask):
    """
    Pull down GBI dropbox content.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return Executable('rsync', message='https://rsync.samba.org/')

    def run(self):
        target = os.path.join(self.taskdir(), 'mirror')
        shellout("mkdir -p {target} && rsync -avzP {src} {target}", src=config.get('gbi', 'scp-src'), target=target)

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
    """
    kind = luigi.Parameter(default='fulltext', description='fulltext or references')

    def requires(self):
        return GBIDropbox()

    def run(self):
        dropbox = GBIDropbox()
        with self.output().open('w') as output:
            if self.kind not in DUMP.get('files'):
                raise RuntimeError('no such fileset: %s, available: %s' % (self.kind, DUMP.get('files')))
            files = DUMP['files'][self.kind]
            for path in files:
                abspath = os.path.join(dropbox.taskdir(), path)
                output.write_tsv(abspath)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)

class GBIDumpDatabaseList(GBITask):
    """
    A list of all database names, as seen they occur in the zipfiles, both dump and updates.
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
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GBIDumpXML(GBITask):
    """
    Extract a single Database from a dump file.
    """
    issue = luigi.Parameter(default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    db = luigi.Parameter(description='name of the database to extract')

    def requires(self):
        return {'dblist': GBIDumpDatabaseList(), '7z': Executable(name='7z', message='http://www.7-zip.org/')}

    def run(self):
        archive = None

        with self.input().get('dblist').open() as handle:
            for row in handle.iter_tsv(cols=('kind', 'path', 'db')):
                if row.db == self.db:
                    archive = row.path

        if archive is None:
            # an non-existent database name amount to an empty dump file
            self.logger.debug('no such db: %s' % self.db)
            with self.output().open('w') as output:
                pass
            return

        dbzip = shellout("7z x -so {archive} {db}.zip 2> /dev/null > {output}", archive=archive, db=self.db)
        output = shellout("""unzip -p {dbzip} \*.xml 2> /dev/null |
                             iconv -f iso-8859-1 -t utf-8 |
                             LC_ALL=C grep -v "^<\!DOCTYPE GENIOS PUBLIC" |
                             LC_ALL=C sed -e 's@<?xml version="1.0" encoding="ISO-8859-1" ?>@@g' |
                             LC_ALL=C sed -e 's@</Document>@<x-origin>{origin}</x-origin><x-group>{group}</x-group><x-issue>{issue}</x-issue></Document>@' |
                             pigz -c >> {output} """, dbzip=dbzip, origin=archive, group=self.group(archive), issue=self.issue)

        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml.gz'))

class GBIDumpIntermediateSchema(GBITask):
    """
    Convert the XML for DB into IS.
    """
    issue = luigi.Parameter(default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    db = luigi.Parameter(description='name of the database to extract')

    def requires(self):
        return {
            'file': GBIDumpXML(issue=self.issue, db=self.db),
            'span-import': Executable(name='span-import'),
            'pigz': Executable(name='pigz'),
        }

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout("span-import -i genios <(unpigz -c {input}) | pigz -c >> {output}", input=self.input().get('file').path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))

class GBIUpdateList(GBITask):
    """
    All GBI files that contain updates since a given date.
    """
    since = luigi.DateParameter(default=DUMP['date'], description='used in filename comparison')
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
    """
    since = luigi.DateParameter(default=DUMP['date'], description='used in filename comparison')
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
        output = shellout("sort -u {input} > {output} && rm {input}", input=stopover)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GBIDatabaseList(GBITask):
    """
    All database names, that occur, either in dump, update.
    """
    since = luigi.DateParameter(default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())
    kind = luigi.Parameter(default='fulltext')

    def requires(self):
        return {'dump': GBIDumpDatabaseList(kind=self.kind),
                'update': GBIUpdateDatabaseList(date=self.date, since=self.since, kind=self.kind)}

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        output = shellout("cat <(cut -f3 {dump}) {update} | sort -u > {output}",
                          dump=self.input().get('dump').path, update=self.input().get('update').path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GBIDatabaseType(GBITask):
    """
    Database-Type mapping generated directly from raw data.
    """
    since = luigi.DateParameter(default=DUMP['date'], description='used in filename comparison')
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
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GBIUpdate(GBITask):
    """
    Concatenate updates.
    """
    since = luigi.DateParameter(default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GBIUpdateList(date=self.date)

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in sorted(handle.iter_tsv(cols=('path',))):
                match = UPDATES.get('pattern').search(row.path)
                if not match:
                    continue
                filedate = match.group(1)
                shellout(""" unzip -p {input} | iconv -f iso-8859-1 -t utf-8 |
                             LC_ALL=C grep -v "^<\!DOCTYPE GENIOS PUBLIC" |
                             LC_ALL=C sed -e 's@<?xml version="1.0" encoding="ISO-8859-1" ?>@@g' |
                             LC_ALL=C sed -e 's@</Document>@<x-origin>{origin}</x-origin><x-group>TBA</x-group><x-issue>{issue}</x-issue></Document>@' >> {output}""",
                             input=row.path, origin=row.path, issue=filedate, output=stopover)

        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))

class GBIUpdateIntermediateSchema(GBITask):
    """
    Convert the combined updates to ischema.
    """
    since = luigi.DateParameter(default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GBIUpdate(since=self.since, date=self.date)

    def run(self):
        output = shellout("""span-import -i genios {input} | pigz -c > {output}""", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))

class GBIUpdateIntermediateSchemaFiltered(GBITask):
    """
    Filter out items for a given database.
    """
    since = luigi.DateParameter(default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())
    db = luigi.Parameter(description='name of the database to extract')

    def requires(self):
        return GBIUpdateIntermediateSchema(since=self.since, date=self.date)

    def run(self):
        output = shellout("""jq -r -c '. | select(.["x.package"] == "{db}")' <(unpigz -c {input}) | pigz -c > {output}""",
                          db=self.db, input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))

class GBIDatabase(GBITask):
    """
    Combine dump and updates for a given database.
    """
    issue = luigi.Parameter(default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    since = luigi.DateParameter(default=DUMP['date'], description='used in filename comparison')
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
            shellout("cat {input} >> {output}", input=target.path, output=stopover)

        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))

class GBIDatabaseTable(GBITask):
    """
    For a given database name, extract the ID and date.
    """
    issue = luigi.Parameter(default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    since = luigi.DateParameter(default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())
    db = luigi.Parameter(description='name of the database to extract')

    def requires(self):
        return GBIDatabase(issue=self.issue, since=self.since, date=self.date, db=self.db)

    def run(self):
        output = shellout(r"""jq -r '[.["finc.record_id"], .["x.indicator"]] | @csv' <(unpigz -c {input}) | tr -d '"' | tr ',' '\t' > {output}""", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GBIISSNDatabase(GBITask):
    """
    Record ISSN per record per database.
    """
    issue = luigi.Parameter(default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    since = luigi.DateParameter(default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())
    db = luigi.Parameter(description='name of the database to extract')

    def requires(self):
        return GBIDatabase(issue=self.issue, since=self.since, date=self.date, db=self.db)

    def run(self):
        output = shellout(r"""jq -r '[.["finc.record_id"], .["rft.issn"][]? // "NOT_AVAILABLE"] | @csv' <(unpigz -c {input}) |
                              tr -d '"' |
                              tr ',' '\t' | awk '{{ print "{db}\t"$0 }}'> {output}""", input=self.input().path, db=self.db)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GBIPublicationTitleDatabase(GBITask):
    """
    Extract publication title per database.
    """
    issue = luigi.Parameter(default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    since = luigi.DateParameter(default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())
    db = luigi.Parameter(description='name of the database to extract')

    def requires(self):
        return GBIDatabase(issue=self.issue, since=self.since, date=self.date, db=self.db)

    def run(self):
        output = shellout(r"""jq -r '[.["finc.record_id"], .["rft.jtitle"]? // "NOT_AVAILABLE"] | @csv' <(unpigz -c {input}) |
                              tr -d '"' |
                              tr ',' '\t' | awk '{{ print "{db}\t"$0 }}' > {output}""", input=self.input().path, db=self.db)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GBIPublicationTitleWrapper(GBITask, luigi.WrapperTask):
    """
    Just a wrapper to prepare publication title lists.
    """
    issue = luigi.Parameter(default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    since = luigi.DateParameter(default=DUMP['date'], description='used in filename comparison')
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
    issue = luigi.Parameter(default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    since = luigi.DateParameter(default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'references': GBIPublicationTitleWrapper(issue=self.issue, since=self.since, date=self.date, kind='references'),
                'fulltext': GBIPublicationTitleWrapper(issue=self.issue, since=self.since, date=self.date, kind='fulltext')}

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for kind, targets in self.input().iteritems():
            for target in targets:
                shellout(""" cat {input} | awk '{{ print "{kind}\t"$0 }}' >> {output} """, input=target.path, output=stopover, kind=kind)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GBIDatabaseISSNWrapper(GBITask, luigi.WrapperTask):
    """
    Just a wrapper to prepare ISSN lists.
    """
    issue = luigi.Parameter(default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    since = luigi.DateParameter(default=DUMP['date'], description='used in filename comparison')
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

class GBIDatabaseWrapper(GBITask, luigi.WrapperTask):
    """
    Just a wrapper to prepare all databases.
    """
    issue = luigi.Parameter(default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    since = luigi.DateParameter(default=DUMP['date'], description='used in filename comparison')
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
    issue = luigi.Parameter(default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    since = luigi.DateParameter(default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())
    kind = luigi.Parameter(default='fulltext')

    def requires(self):
        return GBIDatabaseISSNWrapper(issue=self.issue, since=self.since, date=self.date, kind=self.kind)

    def run(self):
        issn_pattern = re.compile(r'[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9xX]')
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
                            stats['withissn_percentage'] = '%0.2f' % (100.0 / stats['total'] * stats['issn'])
                        else:
                            stats['withissn_percentage'] = 0
                        output.write_tsv(stats['db'], 'ok', stats['total'], stats['withissn_percentage'], stats['issncount'],
                                         stats['issn'], stats['noissn'], stats['valid'], stats['invalid'], stats['skipped'])
                    except Exception as err:
                        output.write_tsv(stats['db'], 'err', 0, 0, 0, 0, 0, 0, 0, 0)
                        self.logger.debug(err)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)

class GBIMergeMaps(GBITask):
    """
    Merge package information (DB, package) and Sigel information into a single
    data structure, where each Sigel is mapped to a list of DB names.

    Requires both (mostly) static files to be available as assets.

    Example:
        {
          "DE-L229": [
            "ZAAA",
            "SBAN",
            "TAWI",
            "BIKE",
            ...
            "INDB"
          ],
          "DE-D161": [
            "ZAAA",
            "SBAN",
            ...
        ...
        }
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        with open(self.assets('gbi_package_sigel_map.json')) as handle:
            sigil_package = json.load(handle)

        merged = collections.defaultdict(set)

        with luigi.File(self.assets('gbi_package_db_map_fulltext.tsv'), format=TSV).open() as handle:
            for row in handle.iter_tsv(cols=('package', 'db')):
                for sigel, packages in sigil_package.iteritems():
                    for package in packages:
                        if package == row.package:
                            merged[sigel].add(row.db)

        with self.output().open('w') as output:
            output.write(json.dumps(merged, cls=SetEncoder))

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='json'), format=TSV)

class GBIPackageList(GBITask):
    """
    Return a list of packages for a given Sigel.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    sigel = luigi.Parameter(default='DE-15')

    def requires(self):
        return GBIMergeMaps(date=self.date)

    def run(self):
        with self.input().open() as handle:
            merged = json.load(handle)

        if self.sigel not in merged:
            raise RuntimeError('unknown isil: %s, available: %s' % (self.sigel, ', '.join(merged.keys())))

        with self.output().open('w') as output:
            for name in merged[self.sigel]:
                output.write_tsv(name)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
