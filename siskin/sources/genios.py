# coding: utf-8
# pylint: disable=C0301

# Copyright 2016 by Leipzig University Library, http://ub.uni-leipzig.de
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
Genios
------

This is a reimplementation of Genios tasks.

* References (fachzeitschriften = FZS)
* Fulltexts (various packages)

* Dump (called reload, monthly)

Examples
--------

* konsortium_sachsen_fachzeitschriften_STS_reload_201609.zip
* konsortium_sachsen_fachzeitschriften_update_20160821070003.zip

Ebooks are reloaded monthy:

* konsortium_sachsen_ebooks_GABA_reload_201607.zip

* konsortium_sachsen_literaturnachweise_wirtschaftswissenschaften_IFOK_reload_201606.zip
* konsortium_sachsen_literaturnachweise_wirtschaftswissenschaften_update_20161122091046.zip

"""

import datetime
import operator
import os
import re
import tempfile

import luigi

from gluish.format import TSV
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.common import Executable
from siskin.task import DefaultTask
from siskin.utils import iterfiles


class GeniosTask(DefaultTask):
    """
    Genios task.
    """
    TAG = 'genios'

    allowed_kinds = set([
        'ebooks',
        'fachzeitschriften',
        'literaturnachweise_psychologie',
        'literaturnachweise_recht',
        'literaturnachweise_sozialwissenschaften',
        'literaturnachweise_technik',
        'literaturnachweise_wirtschaftswissenschaften',
    ])

class GeniosDropbox(GeniosTask):
    """
    Pull down content from FTP.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return Executable('rsync', message='https://rsync.samba.org/')

    def run(self):
        target = os.path.join(self.taskdir(), 'mirror')
        shellout("mkdir -p {target} && rsync {rsync_options} {src} {target}",
                 rsync_options=self.config.get('gbi', 'rsync-options', '-avzP'),
                 src=self.config.get('gbi', 'scp-src'), target=target)

        if not os.path.exists(self.taskdir()):
            os.makedirs(self.taskdir())

        with self.output().open('w') as output:
            for path in iterfiles(target):
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)

class GeniosReloadDates(GeniosTask):
    """
    Extract all reload dates, write them sorted into a file.

    Example output (first 4 columns):

        ...
        |  fachzeitschriften                            | ZVS     | 2016    | 10       |
        |  fachzeitschriften                            | ZWF     | 2016    | 06       |
        |  fachzeitschriften                            | ZWF     | 2016    | 07       |
        |  fachzeitschriften                            | ZWF     | 2016    | 08       |
        |  fachzeitschriften                            | ZWF     | 2016    | 09       |
        |  fachzeitschriften                            | ZWF     | 2016    | 10       |
        |  literaturnachweise_psychologie               | PSYT    | 2016    | 06       |
        |  literaturnachweise_psychologie               | PSYT    | 2016    | 07       |
        |  literaturnachweise_psychologie               | PSYT    | 2016    | 08       |
        |  literaturnachweise_psychologie               | PSYT    | 2016    | 09       |
        |  literaturnachweise_psychologie               | PSYT    | 2016    | 10       |
        ...

    Group names are hardcoded (regex) in this task. List them with:

        $ taskcat GeniosReloadDates | cut -f1|sort -u
        ebooks
        fachzeitschriften
        literaturnachweise_psychologie
        literaturnachweise_recht
        literaturnachweise_sozialwissenschaften
        literaturnachweise_technik
        literaturnachweise_wirtschaftswissenschaften

    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return GeniosDropbox(date=self.date)

    def run(self):
        """
        Reload files are marked with date (years + month).
        """
        pattern = re.compile(
            '.*konsortium_sachsen_('
            'literaturnachweise_psychologie|'
            'literaturnachweise_recht|'
            'literaturnachweise_sozialwissenschaften|'
            'literaturnachweise_technik|'
            'literaturnachweise_wirtschaftswissenschaften|'
            'fachzeitschriften|ebooks)_'
            '([A-Z]*)_reload_(20[0-9][0-9])([01][0-9]).*')

        rows = []

        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                match = pattern.match(row.path)
                if not match:
                    continue
                cols = list(match.groups()) + [row.path]
                rows.append(cols)

        with self.output().open('w') as output:
            for row in sorted(rows):
                output.write_tsv(*row)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)

class GeniosDatabases(GeniosTask):
    """
    Extract a list of database names by --kind. Example output:

    AAA
    AAS
    AATG
    ABAU
    ABES
    ABIL
    ...
    """
    kind = luigi.Parameter(default='fachzeitschriften', description='or: ebooks, literaturnachweise_...')
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return GeniosReloadDates(date=self.date)

    def run(self):
        if self.kind not in GeniosTask.allowed_kinds:
            raise RuntimeError('only these --kind parameters are allowed: %s' % ', '.join(GeniosTask.allowed_kinds))

        dbs = set()
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('kind', 'db', 'year', 'month', 'path')):
                if not row.kind.startswith(self.kind):
                    continue
                dbs.add(row.db)

        with self.output().open('w') as output:
            for db in sorted(dbs):
                output.write_tsv(db)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class GeniosLatest(GeniosTask):
    """
    Get the latest version of all files, belonging to some kind, e.g. FZS.

    Latest FZS is about 10G, takes 10min to build.

    The XML contains an additional element, X-Package, containing the kind,
    e.g. fachzeitschriften, literaturnachweise_technik, ...
        
        ...
        </Copyright>
        <X-Package>ebooks</X-Package></Document>
        ...

    """
    kind = luigi.Parameter(default='fachzeitschriften', description='or: ebooks, literaturnachweise_...')
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return GeniosReloadDates(date=self.date)

    def run(self):
        """
        Map each database name to the latest file. Since the required task is
        sorted, the last entry for a name should be the latest.

        Content of all files is concatenated and gzipped.

        This way, different database can have different reload dates, and we
        still can have a *latest* version without explicit dates.

        TODO: What if the latest file is a partial upload?
        """

        if self.kind not in GeniosTask.allowed_kinds:
            raise RuntimeError('only these --kind parameters are allowed: %s' % ', '.join(GeniosTask.allowed_kinds))

        filemap = {}

        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('kind', 'db', 'year', 'month', 'path')):
                if not row.kind.startswith(self.kind):
                    continue
                filemap[row.db] = row.path

        if not filemap:
            raise RuntimeError('could not file a single file for the specified kind: %s' % self.kind)

        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for name, path in filemap.iteritems():
            shellout("""unzip -p {input} |
                        iconv -f iso-8859-1 -t utf-8 |
                        LC_ALL=C sed -e 's@</Document>@<X-Package>{package}</X-Package></Document>@' |
                        pigz -c >> {output}""", input=path, output=stopover, package=self.kind)

        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml.gz'))
