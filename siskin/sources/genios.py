# coding: utf-8
# pylint: disable=C0301,E1101

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
import os
import re
import tempfile

import luigi

from gluish.format import TSV
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.common import Executable
from siskin.sources.amsl import AMSLFilterConfig
from siskin.task import DefaultTask
from siskin.utils import iterfiles


class GeniosTask(DefaultTask):
    """
    Genios task.

    Allowed set names:

    * ebooks
    * fachzeitschriften
    * literaturnachweise_psychologie
    * literaturnachweise_recht
    * literaturnachweise_sozialwissenschaften
    * literaturnachweise_technik
    * literaturnachweise_wirtschaftswissenschaften

    The database_blacklist lists database names, that should excluded from processing, #9534.

    """
    TAG = '048'

    allowed_kinds = set([
        'ebooks',
        'fachzeitschriften',
        'literaturnachweise_psychologie',
        'literaturnachweise_recht',
        'literaturnachweise_sozialwissenschaften',
        'literaturnachweise_technik',
        'literaturnachweise_wirtschaftswissenschaften',
    ])

    database_blacklist = (
        'AE',
        'ANP',
        'AUTO',
        'AUW',
        'BSTZ',
        'EMAR',
        'FERT',
        'FLUI',
        'FP',
        'FW',
        'HOLZ',
        'INST',
        'KE',
        'KONT',
        'LEDI',
        'MEIN',
        'MF',
        'MUM',
        'NPC',
        'PROD',
        'STHZ',
        'STIF',
        'WEFO',
        'WUV',
    )

    def closest(self):
        return monthly(date=self.date)


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
                # cols contains the [kind, database name, year, month, filename]
                # ['ebooks', 'DFVE', '2016', '09', '.../siskin-data/genios/...reload_201609.zip']

                if cols[1] in self.database_blacklist:
                    self.logger.debug("excluding blacklisted %s from further processing", cols[1])
                    continue

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
    kind = luigi.Parameter(default='fachzeitschriften',
                           description='or: ebooks, literaturnachweise_...')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GeniosReloadDates(date=self.date)

    def run(self):
        if self.kind not in GeniosTask.allowed_kinds:
            raise RuntimeError('only these --kind parameters are allowed: %s' %
                               ', '.join(GeniosTask.allowed_kinds))

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
    kind = luigi.Parameter(default='fachzeitschriften',
                           description='or: ebooks, literaturnachweise_...')
    date = ClosestDateParameter(default=datetime.date.today())

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
            raise RuntimeError('only these --kind parameters are allowed: %s' %
                               ', '.join(GeniosTask.allowed_kinds))

        filemap = {}

        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('kind', 'db', 'year', 'month', 'path')):
                if not row.kind.startswith(self.kind):
                    continue
                filemap[row.db] = row.path

        if not filemap:
            raise RuntimeError(
                'could not file a single file for the specified kind: %s' % self.kind)

        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for _, path in filemap.iteritems():
            shellout(r"""unzip -p {input} |
                        iconv -f iso-8859-1 -t utf-8 |
                        LC_ALL=C grep -v "^<\!DOCTYPE GENIOS PUBLIC" |
                        LC_ALL=C sed -e 's@<?xml version="1.0" encoding="ISO-8859-1" ?>@@g' |
                        LC_ALL=C sed -e 's@</Document>@<X-Package>{package}</X-Package></Document>@' |
                        pigz -c >> {output}""", input=path, output=stopover, package=self.kind)

        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml.gz'))


class GeniosIntermediateSchema(GeniosTask):
    """
    Intermediate schema by kind.

    Warnings (incomplete):

    * 2016/12/20 13:57:58 genios: db is not associated with package: ELEO, using generic default
    * 2016/12/20 13:59:34 genios: db is not associated with package: AKS, using generic default
    * 2016/12/20 14:04:06 genios: db is not associated with package: KTX, using generic default
    * 2016/12/20 14:07:01 genios: db is not associated with package: GJHR, using generic default
    * 2016/12/20 14:08:20 genios: db is not associated with package: MAON, using generic default
    * 2016/12/20 14:10:00 genios: db is not associated with package: TUD, using generic default
    * 2016/12/20 14:11:59 genios: db is not associated with package: FOWE, using generic default
    * 2016/12/20 14:18:52 genios: db is not associated with package: TIAM, using generic default
    * 2016/12/20 14:37:41 genios: db is not associated with package: HTEC, using generic default
    * 2017/02/27 11:49:52 genios: db is not associated with package: TWNE, using generic default
    * 2017/02/27 11:50:01 genios: db is not associated with package: SPI, using generic default
    * 2017/02/27 11:50:16 genios: db is not associated with package: EAWO, using generic default
    * 2017/02/27 11:50:48 genios: db is not associated with package: BRSC, using generic default
    * 2017/02/27 11:50:48 genios: db is not associated with package: SBP, using generic default
    * 2017/02/27 11:50:58 genios: db is not associated with package: AUKO, using generic default
    * 2017/02/27 11:51:00 genios: db is not associated with package: HILA, using generic default
    * 2017/02/27 11:51:01 genios: db is not associated with package: FIME, using generic default
    * 2017/02/27 11:51:01 genios: db is not associated with package: PJCP, using generic default
    * 2017/02/27 11:52:27 genios: db is not associated with package: JNC, using generic default
    * 2017/02/27 11:52:27 genios: db is not associated with package: REMW, using generic default
    * 2017/02/27 11:53:14 genios: db is not associated with package: LAW, using generic default
    * 2017/02/27 11:53:26 genios: db is not associated with package: CME, using generic default
    * 2017/02/27 11:53:26 genios: db is not associated with package: GUG, using generic default
    * 2017/02/27 11:54:47 genios: db is not associated with package: ELEO, using generic default
    * 2017/02/27 11:55:30 genios: db is not associated with package: EIRB, using generic default
    * 2017/02/27 11:56:50 genios: db is not associated with package: PJP, using generic default
    * 2017/02/27 11:57:18 genios: db is not associated with package: AKS, using generic default
    * 2017/02/27 11:57:18 genios: db is not associated with package: IFAM, using generic default
    * 2017/02/27 11:57:27 genios: db is not associated with package: AKA, using generic default
    * 2017/02/27 11:57:27 genios: db is not associated with package: FK, using generic default
    * 2017/02/27 11:57:30 genios: db is not associated with package: ASPA, using generic default
    * 2017/02/27 11:57:31 genios: db is not associated with package: PHAP, using generic default
    * 2017/02/27 12:01:11 genios: db is not associated with package: JER, using generic default
    * 2017/02/27 12:02:30 genios: db is not associated with package: BGWP, using generic default
    * 2017/02/27 12:03:08 genios: db is not associated with package: PGE, using generic default
    * 2017/02/27 12:03:11 genios: db is not associated with package: LB, using generic default
    * 2017/02/27 12:06:31 genios: db is not associated with package: INJO, using generic default
    * 2017/02/27 12:06:35 genios: db is not associated with package: MBP, using generic default
    * 2017/02/27 12:06:50 genios: db is not associated with package: PAPE, using generic default
    * 2017/02/27 12:06:58 genios: db is not associated with package: IGMW, using generic default
    * 2017/02/27 12:07:08 genios: db is not associated with package: BME, using generic default
    * 2017/02/27 12:07:15 genios: db is not associated with package: BUBH, using generic default
    * 2017/02/27 12:07:45 genios: db is not associated with package: FINE, using generic default
    * 2017/02/27 12:08:21 genios: db is not associated with package: SCMW, using generic default
    * 2017/02/27 12:08:11 genios: db is not associated with package: EU, using generic default
    * 2017/02/27 12:08:27 genios: db is not associated with package: EMRE, using generic default
    * 2017/02/27 12:08:41 genios: db is not associated with package: JBS, using generic default
    * 2017/02/27 12:08:48 genios: db is not associated with package: UTIL, using generic default
    * 2017/02/27 12:09:06 genios: db is not associated with package: MUB, using generic default
    * 2017/02/27 12:09:31 genios: db is not associated with package: PVD, using generic default
    * 2017/02/27 12:09:50 genios: db is not associated with package: FMAI, using generic default
    * 2017/02/27 12:10:19 genios: db is not associated with package: TUD, using generic default
    * 2017/02/27 12:10:38 genios: db is not associated with package: DI, using generic default
    * 2017/02/27 12:10:55 genios: db is not associated with package: ISBF, using generic default
    * 2017/02/27 12:11:07 genios: db is not associated with package: AGRI, using generic default
    * 2017/02/27 12:11:07 genios: db is not associated with package: OGR, using generic default
    * 2017/02/27 12:11:09 genios: db is not associated with package: DQ, using generic default
    * 2017/02/27 12:11:46 genios: db is not associated with package: SLO, using generic default
    * 2017/02/27 12:12:12 genios: db is not associated with package: KUCH, using generic default
    * 2017/02/27 12:12:20 genios: db is not associated with package: JPV, using generic default
    * 2017/02/27 12:12:20 genios: db is not associated with package: FOWE, using generic default
    * 2017/02/27 12:13:27 genios: db is not associated with package: AUPR, using generic default
    * 2017/02/27 12:13:39 genios: db is not associated with package: DQW, using generic default
    * 2017/02/27 12:13:43 genios: db is not associated with package: DEBA, using generic default
    * 2017/02/27 12:14:04 genios: db is not associated with package: PHM, using generic default
    * 2017/02/27 12:14:04 genios: db is not associated with package: CAVE, using generic default
    * 2017/02/27 12:14:17 genios: db is not associated with package: EMW, using generic default
    * 2017/02/27 12:14:17 genios: db is not associated with package: DMS, using generic default
    * 2017/02/27 12:14:35 genios: db is not associated with package: PC, using generic default
    * 2017/02/27 12:14:41 genios: db is not associated with package: JGSI, using generic default
    * 2017/02/27 12:15:08 genios: db is not associated with package: FSM, using generic default
    * 2017/02/27 12:15:48 genios: db is not associated with package: MABL, using generic default
    * 2017/02/27 12:17:11 genios: db is not associated with package: LHMW, using generic default
    * 2017/02/27 12:18:40 genios: db is not associated with package: ENER, using generic default
    * 2017/02/27 12:18:48 genios: db is not associated with package: TIAM, using generic default
    * 2017/02/27 12:18:53 genios: db is not associated with package: SOA, using generic default
    * 2017/02/27 12:19:04 genios: db is not associated with package: MBW, using generic default
    * 2017/02/27 12:19:12 genios: db is not associated with package: VD, using generic default
    * 2017/02/27 12:19:14 genios: db is not associated with package: MUTE, using generic default
    * 2017/02/27 12:19:26 genios: db is not associated with package: KONZ, using generic default
    * 2017/02/27 12:20:00 genios: db is not associated with package: BIWI, using generic default
    * 2017/02/27 12:20:00 genios: db is not associated with package: AUMO, using generic default
    * 2017/02/27 12:20:00 genios: db is not associated with package: BTME, using generic default
    * 2017/02/27 12:21:18 genios: db is not associated with package: SAFR, using generic default
    * 2017/02/27 12:21:41 genios: db is not associated with package: BURE, using generic default
    * 2017/02/27 12:22:26 genios: db is not associated with package: KUSO, using generic default
    * 2017/02/27 12:22:41 genios: db is not associated with package: AMW, using generic default
    * 2017/02/27 12:24:20 genios: db is not associated with package: WWON, using generic default
    * 2017/02/27 12:25:05 genios: db is not associated with package: JRW, using generic default
    * 2017/02/27 12:25:05 genios: db is not associated with package: PJLS, using generic default
    * 2017/02/27 12:25:26 genios: db is not associated with package: HPRW, using generic default
    * 2017/02/27 12:25:26 genios: db is not associated with package: REGI, using generic default
    * 2017/02/27 12:25:28 genios: db is not associated with package: DISK, using generic default
    * 2017/02/27 12:25:28 genios: db is not associated with package: KSM, using generic default
    * 2017/02/27 12:26:55 genios: db is not associated with package: OER, using generic default
    * 2017/02/27 12:26:54 genios: db is not associated with package: EAW, using generic default

    Related: "Neue Quellen bzw. Austausch", Mon, Dec 5, 2016 at 12:23 PM, ba54ea7d396a41a2a1281f51bba5d33f
    See also: #9534.

    """
    kind = luigi.Parameter(default='fachzeitschriften',
                           description='or: ebooks, literaturnachweise_...')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return GeniosLatest(kind=self.kind, date=self.date)

    def run(self):
        if not os.path.exists(self.taskdir()):
            os.makedirs(self.taskdir())
        output = shellout("span-import -i genios <(unpigz -c {input}) | pigz -c >> {output}", input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))


class GeniosCombinedIntermediateSchema(GeniosTask):
    """
    Concat all genios files.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        """
        No ebooks.
        """
        return [
            GeniosIntermediateSchema(date=self.date, kind='fachzeitschriften'),
            GeniosIntermediateSchema(
                date=self.date, kind='literaturnachweise_psychologie'),
            GeniosIntermediateSchema(
                date=self.date, kind='literaturnachweise_recht'),
            GeniosIntermediateSchema(
                date=self.date, kind='literaturnachweise_sozialwissenschaften'),
            GeniosIntermediateSchema(
                date=self.date, kind='literaturnachweise_technik'),
            GeniosIntermediateSchema(
                date=self.date, kind='literaturnachweise_wirtschaftswissenschaften'),
        ]

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}",
                     input=target.path, output=stopover)
        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))


class GeniosCombinedExport(GeniosTask):
    """
    Tag with ISILs, then export to various formats.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='solr5vu3', description='export format')

    def requires(self):
        return {
            'file': GeniosCombinedIntermediateSchema(date=self.date),
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


class GeniosISSNList(GeniosTask):
    """
    A list of Genios ISSNs.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'input': GeniosCombinedIntermediateSchema(date=self.date),
            'jq': Executable(name='jq', message='https://github.com/stedolan/jq')
        }

    def run(self):
        _, output = tempfile.mkstemp(prefix='siskin-')
        shellout("""jq -c -r '.["rft.issn"][]?' <(unpigz -c {input}) >> {output} """, input=self.input().get('input').path, output=output)
        shellout("""jq -c -r '.["rft.eissn"][]?' <(unpigz -c {input}) >> {output} """, input=self.input().get('input').path, output=output)
        output = shellout("""LC_ALL=C sort -S35% -u {input} > {output} """, input=output)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
