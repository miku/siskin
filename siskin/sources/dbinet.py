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

Fixed source from the defunct http://www.medien-buehne-film.de/.

About 214M in 163 XML files, no schema, root XML element is called DBClear,
currently the files are named monday.xml.part133, monday.xml.part134, and so
on.

Configuration
-------------

[dbinet]

directory = /path/to/dir/containing/raw/files

"""

import datetime
import glob
import json
import os
import tempfile

import luigi
import pytz
import requests
from luigi.format import Gzip

from gluish.format import TSV
from gluish.utils import shellout
from siskin.task import DefaultTask
from siskin.utils import load_set_from_file


class DBInetTask(DefaultTask):
    """
    Base task, refs #5374, #9913, #11005, #13653.
    """
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
    Combine, prepare, requires xlstproc, sed, jq.
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
    """
    Convert via jq.

    This tasks can create a list of failed documents, with `--failed errdocs.json` flag.

    To create file containing link check results for failed links, run:

        $ taskrm DBInetIntermediateSchema
        $ taskdo DBInetIntermediateSchema --failed errdocs.json
        $ jq -rc '.url[]' errdocs.json | clinker | jq -r 'select(.status != 200) | .link' > urls_to_drop.txt

    Put path to urls_to_drop.txt (one URL per line) into:

        [dbinet]

        exclude-links = urls_to_drop.txt

    """

    urlcheck = luigi.BoolParameter(default=False, significant=False)
    failed = luigi.Parameter(default="", description="write failed JSON to this file", significant=False)

    def requires(self):
        return DBInetJSON()

    def run(self):
        """
        When URLs are checked, only write records with at least one reachable URL.
        """
        output = shellout("jq -rc -f {filter} {input} | pigz -c > {output}",
                          filter=self.assets('80/filter.jq'), input=self.input().path)

        # If we have file to a list of links to exclude, then load this list.
        urls_to_drop = set()

        exclude_file = self.config.get("dbinet", "exclude-links")
        if exclude_file is None or not os.path.exists(exclude_file):
            self.logger.debug(self.__doc__)
        else:
            urls_to_drop = load_set_from_file(exclude_file)

        # Data cleaning.
        dropped_records = 0

        with luigi.LocalTarget(output, format=Gzip).open() as handle:
            with luigi.LocalTarget(is_tmp=True, format=Gzip).open('w') as tmp:
                for i, line in enumerate(handle):
                    line = line.strip()
                    if not line:
                        continue
                    doc = json.loads(line)

                    # Mitigate stutter in result list, refs #13653.
                    series = doc["rft.series"]
                    if series:
                        doc["x.footnotes"] = [doc["rft.series"]]
                        doc["rft.series"] = ""

                    if doc.get("rft.date") in (None, "", "null"):
                        # This is probably a web site.
                        if self.failed:
                            with open(self.failed, "a") as failed:
                                failed.write(json.dumps(doc))
                                failed.write("\n")

                        # If the link is valid, then add current date and
                        # change document format.
                        for url in doc.get("url", []):
                            if url in urls_to_drop:
                                self.logger.debug("dropping document, since link is in exclude list: %s", url)
                                dropped_records += 1
                                continue

                        today = datetime.datetime.now(pytz.timezone('UTC'))
                        doc["rft.date"] = today.strftime("%Y-%m-%d")
                        doc["x.date"] = today.isoformat()
                        doc["finc.format"] = "ElectronicIntegratingResource"
                        doc["finc.genre"] = "document"

                    doc["rft.place"] = [v for v in doc["rft.place"] if v]
                    doc["rft.pub"] = [v for v in doc["rft.pub"] if v]

                    tmp.write(json.dumps(doc))
                    tmp.write("\n")

        luigi.LocalTarget(tmp.path).move(self.output().path)
        self.logger.debug("dropped %s records during cleaning", dropped_records)

        if self.urlcheck:
            raise NotImplementedError()

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)
