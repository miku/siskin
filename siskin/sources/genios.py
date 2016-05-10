#!/usr/bin/env python
# coding: utf-8

#  Copyright 2016 by Leipzig University Library, http://ub.uni-leipzig.de
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

"""
Genios:

* References (FZS)
* Fulltexts (various packages)

* Dump
* Updates

----

There is no package information in the raw XML.

Options:

* Build a database-package relation first.
* During conversion, add package name manually.

$ span-import -i genios -a '{"package": "xyz"}' file.zip

----

Update file structure:

* kons_sachs_fzs_20151118150428_all.zip
* [id]_[group]_YYYYMMDDHHMMSS_[modifier].zip

Groups: fzs, recht, lit

Each record belongs to one database. A database can belong to one or more
groups.

An example shipment:

    /mirror/gbi/SOWI_NOV_2015.zip
    /mirror/gbi/TECHN_NOV_2015.zip
    /mirror/gbi/FZS_NOV_2015.zip
    /mirror/gbi/Recht-Law_NOV_2015.zip
    /mirror/gbi/WIWI_NOV_2015.zip
    /mirror/gbi/PSYN_NOV_2015.zip

FZS are fulltext items. The rest are references.
The fulltext items still have subjects (e.g. WIWI,
etc.) but that does not matter.

Overlapping shipments, e.g. BLIS.zip is contained in SOWI and WIWI.

Other shipments:

    /mirror/gbi/fzs_new_ejournals_wiso2016.zip
    /mirror/gbi/ebooks_20160202.zip
    /mirror/gbi/ebooks-wiso-Nov2015.zip

"""

from gluish.format import TSV
from gluish.utils import shellout
from siskin.common import Executable
from siskin.configuration import Config
from siskin.task import DefaultTask
from siskin.utils import iterfiles, SetEncoder
import collections
import datetime
import glob
import json
import luigi
import os
import re
import tempfile

config = Config.instance()

class GeniosTask(DefaultTask):
    """
    Genios task.
    """
    TAG = 'genios'

class GeniosDropbox(GeniosTask):
    """
    Pull down content.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return Executable('rsync', message='https://rsync.samba.org/')

    def run(self):
        target = os.path.join(self.taskdir(), 'mirror')
        shellout("mkdir -p {target} && rsync {rsync_options} {src} {target}",
                 rsync_options=config.get('gbi', 'rsync-options', '-avzP'),
                 src=config.get('gbi', 'scp-src'), target=target)

        if not os.path.exists(self.taskdir()):
            os.makedirs(self.taskdir())

        with self.output().open('w') as output:
            for path in iterfiles(target):
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)

class GeniosPackageInfo(GeniosTask):
    """
    Derive package information (about serials) from a given FTP mirror only.

    Example output:

        {
          "Recht": [
            "KUSE"
          ],
          "Sozialwissenschaften": [
            "WAO",
            "SOFI",
            "SOLI",
            "DZI",
            "BLIS",
            "IHSL"
          ],
          ...
        }

    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return GeniosDropbox(date=self.date)

    def run(self):
        directory = os.path.join(self.requires().taskdir(), 'mirror', 'gbi')

        metapkgs = {
            'Fachzeitschriften': glob.glob(os.path.join(directory, '*FZS_NOV_2015.zip')),
            'Psychologie': glob.glob(os.path.join(directory, '*PSYN_NOV_2015.zip')),
            'Sozialwissenschaften': glob.glob(os.path.join(directory, '*SOWI_NOV_2015.zip')),
            'Technik': glob.glob(os.path.join(directory, '*TECHN_NOV_2015.zip')),
            'Wirtschaftswissenschaften': glob.glob(os.path.join(directory, '*WIWI_NOV_2015.zip')),
        }

        flatpkgs = {
            'Recht': glob.glob(os.path.join(directory, '*Recht-Law_NOV_2015.zip')),
        }

        updates = {
            'Fachzeitschriften': glob.glob(os.path.join(directory, 'kons_sachs_fzs_*zip')),
            'LIT': glob.glob(os.path.join(directory, 'kons_sachs_lit_*zip')),
            'Recht': glob.glob(os.path.join(directory, 'kons_sachs_recht_*zip')),
        }

        def setify(path, remove_file=True):
            """ Turn a file into a set, each element being on non-empty line. """
            dbs = set()
            with open(output) as handle:
                for db in map(str.strip, handle):
                    if db:
                        dbs.add(db)
            if remove_file:
                os.remove(output)
            return dbs

        def metadbs(path):
            """
            Given the path to a meta-package, return a set of database names.
            """
            output = shellout(r""" unzip -l {input} | grep -Eo "[A-Z]+.zip" | sed -e 's/.zip//g' > {output} """, input=path)
            return setify(output)

        def flatdbs(path):
            """
            Given the path to a flat-package, return a set of database names.
            """
            output = shellout(r""" unzip -p {input} | grep -Eo 'DB="[A-Z]*"' | sed -e 's/DB=//g' | tr -d '"' > {output} """, input=path)
            return setify(output)

        pkgmap = collections.defaultdict(set)

        for package, files in metapkgs.iteritems():
            for path in files:
                for db in metadbs(path):
                    pkgmap[package].add(db)

        for package, files in flatpkgs.iteritems():
            for path in files:
                for db in flatdbs(path):
                    pkgmap[package].add(db)

        for package, files in updates.iteritems():
            for path in files:
                for db in flatdbs(path):
                    pkgmap[package].add(db)

        with self.output().open('w') as output:
            json.dump(pkgmap, output, cls=SetEncoder)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='json'))

class GeniosPackageInfoInverted(GeniosTask):
    """
    Reverse the assignment from pkg name -> db to db -> []packages.

    Example output:

        {
          "IJAR": [
            "Fachzeitschriften"
          ],
          "ZAAA": [
            "Fachzeitschriften"
          ],
          "POPS": [
            "Fachzeitschriften"
          ],
          ...
        }

    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return GeniosPackageInfo(date=self.date)

    def run(self):
        with self.input().open() as handle:
            doc = json.load(handle)

        inverted = collections.defaultdict(set)
        for pkg, dbs in doc.iteritems():
            for db in dbs:
                inverted[db].add(pkg)

        with self.output().open('w') as output:
            json.dump(inverted, output, cls=SetEncoder)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='json'))

class GeniosFZSMap(GeniosTask):
    """
    For the opaque FZS, gather the data from other sources, e.g. Excel files.

    Example output:

        {
          "IJAR": [
            "Sozialwissenschaften"
          ],
          "ZAAA": [
            "Wirtschaftswissenschaften",
            "Psychologie",
            "Sozialwissenschaften"
          ],
          ...
        }

    """

    def run(self):
        mapping = collections.defaultdict(set)
        with open(self.assets('gbi_package_db_map_fulltext.tsv')) as handle:
            for name, database in map(lambda line: line.split('\t'), (line.strip() for line in handle if not line.startswith('#'))):
                mapping[database].add(name)

        with self.output().open('w') as output:
            json.dump(mapping, output, cls=SetEncoder)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='json'))

class GeniosMergedMap(GeniosTask):
    """
    Merge the FZS / other inferred mapping and the static FZS mapping into a single file.

    Example output:

        {
          "IJAR": [
            "Fachzeitschriften",
            "Sozialwissenschaften"
          ],
          "ZAAA": [
            "Wirtschaftswissenschaften",
            "Fachzeitschriften",
            "Psychologie",
            "Sozialwissenschaften"
          ],
          "POPS": [
            "Fachzeitschriften",
            "Psychologie",
            "Sozialwissenschaften"
          ],
          ...
        }

    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return [GeniosFZSMap(), GeniosPackageInfoInverted(date=self.date)]

    def run(self):
        merged = collections.defaultdict(set)

        for target in self.input():
            with target.open() as handle:
                mapping = json.load(handle)
            for db, names in mapping.iteritems():
                for name in names:
                    merged[db].add(name)

        with self.output().open('w') as output:
            json.dump(merged, output, cls=SetEncoder)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='json'))

class GeniosMissingPackageNames(GeniosTask):
    """
    There are packages, which are only FZS. For these, we currently cannot determine licensing status.

    Example output:

        ABAL
        ABKL
        ABMS
        ABSC
        AHGA
        AHOR
        AMIF
        ANP
        AVFB
        BONL
        ....

    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return GeniosMergedMap(date=self.date)

    def run(self):
        with self.input().open() as handle:
            doc = json.load(handle)

        missing = set()

        for db, names in doc.iteritems():
            if len(names) == 1 and names[0] == 'Fachzeitschriften':
                missing.add(db)

        with self.output().open('w') as output:
            for db in sorted(missing):
                output.write_tsv(db)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
