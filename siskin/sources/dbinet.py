# coding: utf-8
# pylint: disable=C0301,E1101

# Copyright 2017 by Leipzig University Library, http://ub.uni-leipzig.de
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
Datenbank Internetquellen, #5374, #9913, #11005.

Fixed source from a single file from a defunct http://www.medien-buehne-film.de/.

Configuration
-------------

[dbinet]

directory = /path/to/dir/containing/raw/files

"""

import glob
import json
import os
import tempfile

import luigi
import requests
from gluish.format import TSV
from gluish.utils import shellout

from siskin.task import DefaultTask


class DBInetTask(DefaultTask):
    """ Base task. """
    TAG = "80"


class DBInetFiles(DBInetTask):
    """
    A list of raw files.
    """

    def run(self):
        directory = self.config.get('dbinet', 'directory')
        with self.output().open('w') as output:
            for path in sorted(glob.glob(os.path.join(directory, "*xml.part*"))):
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)


class DBInetJSON(DBInetTask):
    """
    Combine, prepare.
    """

    def requires(self):
        return DBInetFiles()

    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                shellout("""python {xmltojson} <(xsltproc {xsl} <(sed -e 's/&#.;//g' -e 's/&#..;//g' {input})) /dev/stdout |
                            jq -cr ".list[][]" >> {output} """,
                         xmltojson=self.assets('80/xmltojson.py'),
                         xsl=self.assets('80/convert.xsl'), input=row.path, output=stopover)

        luigi.LocalTarget(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))


class DBInetIntermediateSchema(DBInetTask):
    """ Convert via jq. Filter out dead links (takes a while). """

    urlcheck = luigi.BoolParameter(default=False, significant=False)

    def requires(self):
        return DBInetJSON()

    def run(self):
        """ When URLs are checked, only write records with at least one reachable URL. """
        output = shellout("jq -rc -f {filter} {input} > {output}",
                          filter=self.assets('80/filter.jq'), input=self.input().path)

        if not self.urlcheck:
            luigi.LocalTarget(output).move(self.output().path)
            return

        with luigi.LocalTarget(output).open() as handle:
            with self.output().open('w') as output:
                for line in handle:
                    doc = json.loads(line)
                    okurls = []
                    for url in doc.get("url"):
                        try:
                            resp = requests.get(url, timeout=20)
                            self.logger.debug("[%s] %s", resp.status_code, url)
                            if resp.status_code >= 400:
                                continue
                            okurls.append(url)
                        except (requests.exceptions.ConnectionError,
                                requests.exceptions.ReadTimeout,
                                requests.exceptions.TooManyRedirects) as err:
                            self.logger.debug(err)
                            continue

                    if len(okurls) == 0:
                        continue

                    doc["urls"] = okurls
                    output.write(json.dumps(doc))
                    output.write("\n")

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))
