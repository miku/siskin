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
"""

from __future__ import print_function

import json
import re

import luigi

from gluish.format import Gzip
from gluish.utils import shellout
from siskin.sources.amsl import AMSLFilterConfig
from siskin.task import DefaultTask


class SpringerTask(DefaultTask):
    TAG = 'springer'


class SpringerProvided(SpringerTask, luigi.ExternalTask):
    """
    Provided.
    """

    def output(self):
        return luigi.LocalTarget(path=self.config.get('springer', 'intermediate-schema-file'), format=Gzip)


class SpringerCleanFields(SpringerTask):
    """
    Clean abstracts, refs #...
    """

    def requires(self):
        return SpringerProvided()

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

    def requires(self):
        return SpringerCleanFields()

    def output(self):
        return self.input()


class SpringerTagged(SpringerTask):
    """
    Tag records with ISIL.
    """

    def requires(self):
        return {
            'file': SpringerCleanFields(),
            'config': AMSLFilterConfig(),
        }

    def run(self):
        output = shellout("span-tag -c {config} <(unpigz -c {input}) | pigz -c > {output}",
                          config=self.input().get('config').path,
                          input=self.input().get('file').path)

        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'), format=Gzip)


class SpringerExport(SpringerTask):
    """
    Solr importable.
    """
    format = luigi.Parameter(default='solr5vu3', description="solr5vu3, formeta")

    def requires(self):
        return SpringerTagged()

    def run(self):
        output = shellout("span-export <(unpigz -c {input}) | pigz -c > {output}", input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))
