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

username = nana
password = s3cret
url = http://export.com/intermediate.file.gz
"""

from __future__ import print_function

import datetime
import re

import luigi
import six
import ujson as json
from gluish.format import Gzip
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.decorator import deprecated
from siskin.sources.amsl import AMSLFilterConfig
from siskin.task import DefaultTask


class SpringerTask(DefaultTask):
    TAG = 'springer'

    def closest(self):
        return weekly(date=self.date)


class SpringerProvided(SpringerTask, luigi.ExternalTask):
    """
    Provided. This is deprecated via #6647. Data access via download, see: SpringerDownload.
    """

    @deprecated
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


class SpringerIssue11557(SpringerTask):
    """
    Via 5994#note-37, finc.mega_collection is now multi-valued, refs #11557.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return SpringerDownload(date=self.date)

    def run(self):
        output = shellout("""
            jq -rc 'del(.["finc.AIRecordType"]) | del(.["AIAccessFacet"]) | .["finc.mega_collection"] = [.["finc.mega_collection"]]' < <(unpigz -c {input}) | pigz -c > {output}
        """, input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)


class SpringerCleanFields(SpringerTask):
    """
    As per Google Hangout 2017-09-08 we define "exchange-ready" as
    "no-post-processing-required". Hence this task marked deprecated, use
    SpringerDownload task instead.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return SpringerDownload(date=self.date)

    @deprecated
    def run(self):
        striptags = lambda s: re.sub(r'\$\$[^\$]*\$\$', '', re.sub(r'<[^>]*>', '', s))
        with self.input().open() as handle:
            with self.output().open('w') as output:
                for line in handle:
                    doc = json.loads(line)
                    doc['abstract'] = striptags(doc.get('abstract', '').encode('utf-8'))
                    doc['rft.atitle'] = striptags(doc.get('rft.atitle', '').encode('utf-8'))
                    doc['x.subjects'] = [striptags(subj.encode('utf-8')) for subj in doc.get('x.subjects', [])]

                    # #5994, #note-37 / https://git.io/v5ZNu requests
                    # multi-valued mega_collection, rev: https://git.io/v5ZNl.
                    # Turn value into list, if it is not already.
                    if isinstance(doc['finc.mega_collection'], six.string_types):
                        doc['finc.mega_collection'] = [doc['finc.mega_collection']]

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
        return SpringerIssue11557(date=self.date)

    def output(self):
        return self.input()
