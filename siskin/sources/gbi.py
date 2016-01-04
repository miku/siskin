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

ftp-host = host.name
ftp-username = username
ftp-password = password
ftp-path = /
ftp-pattern = some*glob*pattern.zip

scp-src = username@ftp.example.de:/home/gbi

dump = /mirror/gbi/zy.zip /mirror/gbi/zz.zip
"""

from gluish.format import TSV
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

UPDATE_FILENAME_REGEX = re.compile(r'kons_sachs_[a-z]*_([0-9]{14,14})_.*.zip')
DUMP_TAG = '20151102T000000'

class GBITask(DefaultTask):
    TAG = '048'

    def closest(self):
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
    List of files, that contains itself zip files.
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
    def run(self):
        dbox = GBIDropbox()
        with self.output().open('w') as output:
            for path in config.get('gbi', 'dump').split():
                output.write_tsv(os.path.join(dbox.taskdir(), path))

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)

class GBIMatryoshka(GBITask):
    """
    Unzips nested zip files into a single XML file.

    The DB attribute should carry the originating filename, e.g. DB="BLIS" for BLIS.ZIP.

    The outer zipfile name is injected as `<x-origin>`.

    The `<x-group>` is derived from the outer zipfile name.

    Unzip v6 (w/ 7z) or higher is required.
    """
    issue = luigi.Parameter(default=DUMP_TAG, description='tag to use as artificial "Dateissue" for dump')

    def requires(self):
        """
        Dump is a list of nested zip files, beloging to a dump,
        might come from scanning the file system (GBIZipWithZips)
        or configuration (GBIDump).
        """
        return {'dump': GBIDump(), 'sync': GBIDropbox(), '7z': Executable(name='7z', message='http://www.7-zip.org/')}

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().get('dump').open() as handle:
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
    All GBI files that contain updates.
    """
    since = luigi.DateParameter(default=datetime.date(2015, 11, 1), description='used in filename comparison')
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
    Collect all updates since a given date in a single file.
    """
    since = luigi.DateParameter(default=datetime.date(2015, 11, 1), description='used in filename comparison')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GBIUpdateList(date=self.date, since=self.since)

    def run(self):
        """
        Files look like: kons_sachs_XYZ_20150508090205_Y.zip
        We extract the date string and compare the filedate to `self.since`.
        """
        _, stopover = tempfile.mkstemp(prefix='siskin-')

        with self.input().open() as handle:
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

class GBIDumpRawIntermediateSchema(GBITask):
    """
    Convert dump and updates into a single preliminary intermediate schema.
    """
    issue = luigi.Parameter(default=DUMP_TAG, description='tag to use as artificial "Dateissue" for dump')

    def requires(self):
        return GBIMatryoshka(issue=self.issue)

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout("span-import -i genios <(unpigz -c {input}) | pigz -c >> {output}", input=self.input().path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))

class GBIUpdatesRawIntermediateSchema(GBITask):
    """
    Convert dump and updates into a single preliminary intermediate schema.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GBIUpdates(date=self.date)

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout("span-import -i genios {input} | pigz -c >> {output}", input=self.input().path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))

class GBIRawIntermediateSchema(GBITask):
    """
    Convert dump and updates into a single preliminary intermediate schema.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return [GBIUpdatesRawIntermediateSchema(date=self.date),
                GBIDumpRawIntermediateSchema(date=self.date)]

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))

class GBISnapshotLines(GBITask):
    """
    Extract the line numbers that make up the current snapshot.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GBIRawIntermediateSchema(date=self.date)

    def run(self):
        output = shellout(r"""jq -r '[.["finc.record_id"], .["x.indicator"]] | @csv' <(unpigz -c {input})
                              | tr -d '"'
                              | sed -e 's/,/\t/g'
                              | awk '{{print NR "\t"$0}}'
                              | LC_ALL=C sort -S50% -nk2,2 -nk3,3 -r
                              | LC_ALL=C sort -S50% -nrk2,2 -u
                              | LC_ALL=C cut -f1
                              | LC_ALL=C sort -S50% -n > {output} """, input=self.input().path)

        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path('ldj.gz'))

#
# Below tasks are DEPRECATED and will be removed shortly.
#

class GBIGroup(GBITask):
    """ Find groups (subdirs). These are not part of the metadata. """
    date = ClosestDateParameter(default=datetime.date.today())
    group = luigi.Parameter(default='wiwi', description='wiwi, fzs, sowi, recht')

    def requires(self):
        return GBISync(date=self.date)

    def run(self):
        with self.input().open() as handle:
            with self.output().open('w') as output:
                for row in handle.iter_tsv(cols=('path',)):
                    group = row.path.split('/')[-2]
                    if group == self.group:
                        output.write_tsv(row.path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'), format=TSV)

class GBIXMLGroup(GBITask):
    """
    Extract all XML files and concat them. Inject a <x-group> tag, which
    carries the "collection" from the directory name into the metadata.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    group = luigi.Parameter(default='wiwi', description='wiwi, fzs, sowi, recht')

    def requires(self):
       return GBIGroup(date=self.date, group=self.group)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                shellout("""unzip -p {path} \*.xml 2> /dev/null |
                            iconv -f iso-8859-1 -t utf-8 |
                            LC_ALL=C grep -v "^<\!DOCTYPE GENIOS PUBLIC" |
                            LC_ALL=C sed -e 's@<?xml version="1.0" encoding="ISO-8859-1" ?>@@g' |
                            LC_ALL=C sed -e 's@</Document>@<x-group>{group}</x-group></Document>@' >> {output}""",
                         output=stopover, path=row.path, group=self.group,
                         ignoremap={1: 'OK', 9: 'ignoring broken zip'})
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'), format=TSV)

class GBIXML(GBITask):
    """
    Extract all XML files and concat them. Inject a <x-group> tag, which
    carries the "collection" from the directory name into the metadata.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        """ Note: ingore broken 'sowi' for now. """
        return [GBIXMLGroup(date=self.date, group=group) for group in ['wiwi', 'fzs', 'recht']]

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'), format=TSV)

class GBIIntermediateSchema(GBITask):
    """
    Convert GBI to intermediate format via span.
    If group is 'all', create intermediate schema of all groups.
    """

    date = ClosestDateParameter(default=datetime.date.today())
    group = luigi.Parameter(default='all', description='wiwi, fzs, sowi, recht')

    def requires(self):
        span = Executable(name='span-import', message='http://git.io/vI8NV')
        if self.group == "all":
            return {'span': span, 'file': GBIXML(date=self.date)}
        else:
            return {'span': span, 'file': GBIXMLGroup(date=self.date, group=self.group)}

    @timed
    def run(self):
        output = shellout("span-import -i genios {input} > {output}", input=self.input().get('file').path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class GBIISSNList(GBITask):
    """ A list of JSTOR ISSNs. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GBIIntermediateSchema(date=self.date)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout("""jq -r '.["rft.issn"][]' {input} 2> /dev/null >> {output} """, input=self.input().path, output=stopover)
        shellout("""jq -r '.["rft.eissn"][]' {input} 2> /dev/null >> {output} """, input=self.input().path, output=stopover)
        output = shellout("""sort -u {input} > {output} """, input=stopover)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
