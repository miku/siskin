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
import datetime
import luigi
import os
import re
import shutil
import tempfile
import zipfile

config = Config.instance()

# Pattern for files containing updates. Change processing code too, if you change this pattern.
UPDATE_FILENAME_REGEX = re.compile(r'kons_sachs_[a-z]*_([0-9]{14,14})_.*.zip')

# DUMP_DATE is used to ignore earlier updates
DUMP_DATE = datetime.date(2015, 11, 1)

# Files belonging to a dump, relative to GBIDropbox.taskdir()
DUMP_FILES = [
    'mirror/gbi/FZS_NOV_2015.zip',
    'mirror/gbi/PSYN_NOV_2015.zip',
    'mirror/gbi/SOWI_NOV_2015.zip',
    'mirror/gbi/TECHN_NOV_2015.zip',
    'mirror/gbi/WIWI_NOV_2015.zip',
]

# derived string representation
DUMP_TAG = DUMP_DATE.strftime('%Y%m%dT%H%M%S')

class GBITask(DefaultTask):
    TAG = '048'

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

class GBIZipWithZips(GBITask):
    """
    List files, that contain itself zip files.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GBIDropbox(date=self.date)

    def run(self):
        with self.input().open() as handle:
            with self.output().open('w') as output:
                for row in handle.iter_tsv(cols=('path',)):
                    if row.path.endswith('.zip'):
                        try:
                            zf = zipfile.ZipFile(row.path)
                            if any([fn.endswith(".zip") for fn in zf.namelist()]):
                                output.write_tsv(row.path)
                        except zipfile.BadZipfile as err:
                            self.logger.warn("%s: %s" % (row.path, err))

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)

class GBIDump(GBITask):
    """
    List of GBI dump nested zips relative to the GBIDropbox taskdir.
    """
    def requires(self):
        return GBIDropbox()

    def run(self):
        dropbox = GBIDropbox()
        with self.output().open('w') as output:
            for path in DUMP_FILES:
                abspath = os.path.join(dropbox.taskdir(), path)
                output.write_tsv(abspath)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)

class GBIGroupList(GBITask):
    """
    Create a simple two column list of groups and DBNAME.
    """

    def requires(self):
        return GBIDump()

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                group = self.group(row.path)
                shellout(r""" unzip -l {input} | awk '{{print $4}}' | grep "zip$" | cut -d '.' -f1 | awk '{{ print "{group}\t"$0 }}' >> {output}""",
                         group=group, input=row.path, output=stopover)

        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GBIMatryoshka(GBITask):
    """
    Unzips nested zip files into a *single* XML file. The "DB" attribute should carry
    the originating filename, e.g. DB="BLIS" for BLIS.ZIP. The outer zipfile name is
    injected as `<x-origin>`. The `<x-group>` is derived from the outer zipfile
    name via `self.group(filename)`.
    """
    issue = luigi.Parameter(default=DUMP_TAG, description='tag to use as artificial "Dateissue" for dump')

    def requires(self):
        """
        Dump is a list of nested zip files, beloging to a dump,
        might come from scanning the file system (GBIZipWithZips)
        or configuration (GBIDump).
        """
        return {'filelist': GBIDump(), '7z': Executable(name='7z', message='http://www.7-zip.org/')}

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().get('filelist').open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                dirname = tempfile.mkdtemp(prefix='siskin-')
                shellout("7z x -o{dir} {zipfile}", dir=dirname, zipfile=row.path)
                for path in iterfiles(dirname):
                    if not path.endswith('.zip'):
                        continue
                    origin = os.path.basename(row.path)
                    shellout("""unzip -p {zipfile} \*.xml 2> /dev/null |
                                iconv -f iso-8859-1 -t utf-8 |
                                LC_ALL=C grep -v "^<\!DOCTYPE GENIOS PUBLIC" |
                                LC_ALL=C sed -e 's@<?xml version="1.0" encoding="ISO-8859-1" ?>@@g' |
                                LC_ALL=C sed -e 's@</Document>@<x-origin>{origin}</x-origin><x-group>{group}</x-group><x-issue>{issue}</x-issue></Document>@' |
                                pigz -c >> {stopover} """, zipfile=path, stopover=stopover,
                                origin=origin, group=self.group(origin), issue=self.issue)
                shutil.rmtree(dirname)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml.gz'))

class GBIUpdateList(GBITask):
    """
    All GBI files that contain updates since a given date (cf. DUMP_DATE).
    """
    since = luigi.DateParameter(default=DUMP_DATE, description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GBIDropbox(date=self.date)

    def run(self):
        """
        TODO(miku): externalize the pattern.
        """
        since = self.since.strftime('%Y%m%d%H%M%S')

        with self.input().open() as handle:
            with self.output().open('w') as output:
                for row in sorted(handle.iter_tsv(cols=('path',))):
                    match = UPDATE_FILENAME_REGEX.search(row.path)
                    if not match:
                        continue
                    filedate = match.group(1)
                    if filedate < since:
                        continue
                    output.write_tsv(row.path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)

class GBIUpdates(GBITask):
    """
    Collect all updates since a given date in a single XML file.
    Inject `<x-origin>`, `<x-group>` and `<x-issue>` accordingly.
    """
    since = luigi.DateParameter(default=datetime.date(2015, 11, 1), description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'filelist': GBIUpdateList(date=self.date, since=self.since),
            'iconv': Executable(name='iconv'),
            'unzip': Executable(name='unzip'),
        }

    def run(self):
        """
        Files look like: kons_sachs_XYZ_20150508090205_Y.zip
        """
        _, stopover = tempfile.mkstemp(prefix='siskin-')

        with self.input().get('filelist').open() as handle:
            for row in sorted(handle.iter_tsv(cols=('path',))):
                match = UPDATE_FILENAME_REGEX.search(row.path)
                filedate = match.group(1)
                isodate = filedate[:8] + "T" + filedate[8:]
                origin = os.path.basename(row.path)
                shellout("""unzip -p {zipfile} \*.xml 2> /dev/null |
                            iconv -f iso-8859-1 -t utf-8 |
                            LC_ALL=C grep -v "^<\!DOCTYPE GENIOS PUBLIC" |
                            LC_ALL=C sed -e 's@<?xml version="1.0" encoding="ISO-8859-1" ?>@@g' |
                            LC_ALL=C sed -e 's@</Document>@<x-origin>{origin}</x-origin><x-group>{group}</x-group><x-issue>{issue}</x-issue></Document>@' >> {stopover} """,
                            zipfile=row.path, stopover=stopover, origin=origin, group=self.group(origin), issue=isodate)

        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))

class GBIDumpIntermediateSchema(GBITask):
    """
    Convert a dump into a single preliminary intermediate schema.
    """
    issue = luigi.Parameter(default=DUMP_TAG, description='tag to use as artificial "Dateissue" for dump')

    def requires(self):
        return {
            'file': GBIMatryoshka(issue=self.issue),
            'span-import': Executable(name='span-import'),
            'pigz': Executable(name='pigz'),
        }

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout("span-import -i genios <(unpigz -c {input}) | pigz -c >> {output}", input=self.input().get('file').path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))

class GBIUpdateIntermediateSchema(GBITask):
    """
    Convert updates into a single preliminary intermediate schema.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'file': GBIUpdates(date=self.date),
            'span-import': Executable(name='span-import'),
            'pigz': Executable(name='pigz'),
        }

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout("span-import -i genios {input} | pigz -c >> {output}", input=self.input().get('file').path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))

class GBIDumpIndicator(GBITask):
    """
    Extract id and indicator for dump.

    1   dump    ai-48-QkxJU19fMTk4ODQyNzUwNjcxNTEyMjA5NzIxMTQ3OTE0NDUxODQ   20151102T000000
    2   dump    ai-48-QkxJU19fMTk4ODQyODQwODA1MTgxODEyMTEzNjE4MTgyMDIxMTQ   20151102T000000
    3   dump    ai-48-QkxJU19fMTk4ODQyOTMxMTYxODE0NDUxNDU5MTQxMTE1MTMxMzU   20151102T000000
    4   dump    ai-48-QkxJU19fMTk4ODE0MTA2NDkxNTE0MTE2MjEyMTI5MzE5NTMyMDE   20151102T000000
    5   dump    ai-48-QkxJU19fMTk4ODE2MzA2NjE1MTg1MTkxNTE4MTE4OTIwODEzNTI   20151102T000000
    ...
    """
    issue = luigi.Parameter(default=DUMP_TAG, description='tag to use as artificial "Dateissue" for dump')

    def requires(self):
        return {
            'file': GBIDumpIntermediateSchema(issue=self.issue),
            'jq': Executable(name='jq'),
            'pigz': Executable(name='pigz'),
        }

    def run(self):
        output = shellout(r"""jq -r '[.["finc.record_id"], .["x.indicator"]] | @csv' <(unpigz -c {input}) |
                              tr -d '"' | tr , '\t' | awk '{{print NR"\tdump\t"$0}}' | pigz -c > {output}""",
                          input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='tsv.gz'), format=Gzip)

class GBIUpdateIndicator(GBITask):
    """
    Extract id and indicator from update.

    1   update  ai-48-VVJfX1VSLjIwMTUuMjEuTC4wMQ    20151101T072840
    2   update  ai-48-RklOUl9fRlIuMjAxNS4yMS5MLjAx  20151101T072840
    3   update  ai-48-RElCQV9fMjAxNTExMDIxNg    20151101T072840
    4   update  ai-48-RElCQV9fMjAxNTExMDIyMw    20151101T072840
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GBIUpdatesRawIntermediateSchema(date=self.date)

    def run(self):
        output = shellout(r"""jq -r '[.["finc.record_id"], .["x.indicator"]] | @csv' <(unpigz -c {input}) |
                              tr -d '"' | tr , '\t' | awk '{{print NR"\tupdate\t"$0}}' | pigz -c > {output}""",
                          input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='tsv.gz'), format=Gzip)

class GBIIndicator(GBITask):
    """
    Combine indicators.
    """
    issue = luigi.Parameter(default=DUMP_TAG, description='tag to use as artificial "Dateissue" for dump')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return [GBIUpdateIndicator(date=self.date),
                GBIDumpIndicator(issue=self.issue)]

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='tsv.gz'), format=Gzip)

class GBIIntermediateSchema(GBITask):
    """
    A shapshot of GBI in intermediate format. Generation takes a few minutes on an update.
    """
    issue = luigi.Parameter(default=DUMP_TAG, description='tag to use as artificial "Dateissue" for dump')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'indicator': GBIIndicator(date=self.date, issue=self.issue),
                'update': GBIUpdateIntermediateSchema(date=self.date),
                'dump': GBIDumpIntermediateSchema(issue=self.issue),
                'pigz': Executable(name='pigz'),
                'filterline': Executable(name='filterline', message='https://github.com/miku/filterline')}

    def run(self):
        """
        Get a list of lines `from_dump` and `from_update`, that should be kept, then use
        `filterline` to extract these lines quickly.
        """
        output = shellout("""LC_ALL=C sort -S50% -k3,3 -k4,4 -r <(unpigz -c {input}) | LC_ALL=C sort -S50% -uk3,3 | pigz -c > {output} """, input=self.input().get('indicator').path)

        from_dump = shellout("""grep "[[:space:]]dump[[:space:]]" <(unpigz -c {input}) | cut -f1 | LC_ALL=C sort -nu | pigz -c > {output}""", input=output)
        from_update = shellout("""grep "[[:space:]]update[[:space:]]" <(unpigz -c {input}) | cut -f1 | LC_ALL=C sort -nu | pigz -c > {output}""", input=output)

        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout("filterline <(unpigz -c {lines}) <(unpigz -c {input}) | pigz -c >> {output} ", input=self.input().get('dump').path, lines=from_dump, output=stopover)
        shellout("filterline <(unpigz -c {lines}) <(unpigz -c {input}) | pigz -c >> {output} ", input=self.input().get('update').path, lines=from_update, output=stopover)

        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))

class GBIISSNList(GBITask):
    """
    Generate a list of ISSNs.
    """
    issue = luigi.Parameter(default=DUMP_TAG, description='tag to use as artificial "Dateissue" for dump')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GBIIntermediateSchema(issue=self.issue, date=self.date)

    @timed
    def run(self):
        output = shellout("""jq -r '.["rft.issn"][]' <(unpigz -c {input}) 2> /dev/null | sort -u >> {output} """, input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
