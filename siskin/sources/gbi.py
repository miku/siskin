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

UPDATES = {
    # Pattern for files containing updates. Change processing code too, if you change this pattern.
    "pattern": re.compile(r'kons_sachs_[a-z]*_([0-9]{14,14})_.*.zip')
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
    DUMPTAG = DUMP['date'].strftime("%Y%m%dT%H%M%S")

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
    kind = luigi.Parameter(default='fulltext', description='fulltext or reference')

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

class GBIDatabaseList(GBITask):
    """
    A list of all database names, as seen they occur in the zipfiles.
    """
    def requires(self):
        return {'fulltext': GBIDump(kind='fulltext'), 'references': GBIDump(kind='references')}

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for kind, target in self.input().iteritems():
            with target.open() as handle:
                for row in handle.iter_tsv(cols=('path',)):
                    shellout(r""" unzip -l {input} | awk '{{print $4}}' | grep "zip$" | cut -d '.' -f1 | awk '{{ print "{kind}\t{input}\t"$0 }}' >> {output}""",
                             kind=kind, input=row.path, output=stopover)

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
        return {'dblist': GBIDatabaseList(), '7z': Executable(name='7z', message='http://www.7-zip.org/')}

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

    def requires(self):
        return GBIDropbox(date=self.date)

    def run(self):
        since = self.since.strftime('%Y%m%d%H%M%S')

        with self.input().open() as handle:
            with self.output().open('w') as output:
                for row in sorted(handle.iter_tsv(cols=('path',))):
                    match = UPDATES.get('pattern').search(row.path)
                    if not match:
                        continue
                    filedate = match.group(1)
                    if filedate < since:
                        continue
                    output.write_tsv(row.path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)

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

class GBIAllDatabases(GBITask, luigi.WrapperTask):
    """
    Just a wrapper to prepare all databases.
    """
    issue = luigi.Parameter(default=GBITask.DUMPTAG, description='tag to use as artificial "Dateissue" for dump')
    since = luigi.DateParameter(default=DUMP['date'], description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        databases = set()

        with luigi.File(self.assets('gbi_package_db_map_fulltext.tsv'), format=TSV).open() as handle:
            for row in handle.iter_tsv(cols=('package', 'db')):
                databases.add(row.db)

        with luigi.File(self.assets('gbi_package_db_map_refs.tsv'), format=TSV).open() as handle:
            for row in handle.iter_tsv(cols=('package', 'db')):
                databases.add(row.db)

        databases.remove('DST')
        databases.remove('EXFO')

        for name in databases:
            yield GBIDatabase(issue=self.issue, since=self.since, date=self.date, db=name)

    def output(self):
        return self.input()

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
