# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301

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
Springer (testing).

Config
------

[springer]

intermediate-schema-file = /path/to/file # must be gzip compressed

username = nana
password = s3cret
url = http://export.com/intermediate.file.gz
"""

from __future__ import print_function

import datetime
import json
import re

import luigi

from gluish.format import Gzip
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.sources.amsl import AMSLFilterConfig
from siskin.task import DefaultTask


class SpringerTask(DefaultTask):
    TAG = 'springer'

    def closest(self):
        return monthly(date=self.date)


class SpringerProvided(SpringerTask, luigi.ExternalTask):
    """
    Provided.
    """

    def output(self):
        return luigi.LocalTarget(path=self.config.get('springer', 'intermediate-schema-file'), format=Gzip)


class SpringerDownload(SpringerTask):
    """
    Attempt to download file from a configured URL.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout(""" curl --fail -v -u {username}:{password} "{url}" > {output} """,
                          username=self.config.get('springer', 'username'),
                          password=self.config.get('springer', 'password'),
                          url=self.config.get('springer', 'url'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)


class SpringerCleanFields(SpringerTask):
    """
    Clean abstracts, refs #...
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return SpringerDownload(date=self.date)

    def run(self):
        striptags = lambda s: re.sub(r'\$\$[^\$]*\$\$', '', re.sub(r'<[^>]*>', '', s))
        with self.input().open() as handle:
            with self.output().open('w') as output:
                for line in handle:
                    doc = json.loads(line)
                    doc['abstract'] = striptags(doc.get('abstract', '').encode('utf-8'))
                    doc['rft.atitle'] = striptags(doc.get('rft.atitle', '').encode('utf-8'))
                    doc['x.subjects'] = [striptags(subj.encode('utf-8')) for subj in doc.get('x.subjects', [])]
                    output.write(json.dumps(doc))
                    output.write("\n")

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)


class SpringerIntermediateSchema(SpringerTask, luigi.WrapperTask):
    """
    Just the cleaned version.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return SpringerCleanFields(date=self.date)

    def output(self):
        return self.input()
