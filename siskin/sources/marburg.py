# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301

# Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
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
Medienwissenschaft, Rezensionen, Reviews. Archiv, Marburg, refs #5486, #11005.

Previously:

* http://archiv.ub.uni-marburg.de/ep/0002/oai

Now (2018-04-23):

* http://archiv.ub.uni-marburg.de/ubfind/OAI/Server

"""

import datetime
import json
import os
import tempfile

import luigi
import xmltodict
from gluish.format import Gzip
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.sources.amsl import AMSLFilterConfig
from siskin.task import DefaultTask


class MarburgTask(DefaultTask):
    """ Base task for Marburg. """
    TAG = '73'

    def closest(self):
        return weekly(self.date)


class MarburgCombine(MarburgTask):
    """
    Harvest and combine into a single file.

    XXX: NLM format has been discontinued as of 2018-01-01, refs #5486.
    XXX: Maybe datacite.
    """

    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='nlm')

    def run(self):
        endpoint = "http://archiv.ub.uni-marburg.de/ubfind/OAI/Server"
        shellout("metha-sync -format nlm {endpoint}", endpoint=endpoint)
        output = shellout("metha-cat -format nlm {endpoint} > {output}", endpoint=endpoint)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(self.path())


class MarburgMarc(MarburgTask):
    """
    Convert XML to Marc.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='datacite')

    def requires(self):
        return MarburgCombine(format=self.format)

    def run(self):
        output = shellout("python {script} {input} > {output}",
                          script=self.assets("73/73_marcbinary.py"), input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(self.path(ext='mrc'))


class MarburgJSON(MarburgTask):
    """
    Convert XML to JSON in one go. Preparation, so we can use jq(1).
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return MarburgCombine(date=self.date)

    def run(self):
        with self.input().open() as handle:
            with self.output().open('w') as output:
                json.dump(xmltodict.parse(handle.read()), output)

    def output(self):
        return luigi.LocalTarget(self.path(ext='json'))


class MarburgIntermediateSchema(MarburgTask):
    """
    Convert to intermediate schema.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return MarburgJSON(date=self.date)

    def run(self):
        output = shellout(""" cat {input} | \
                              jq '.Records.Record[]|.metadata.article' | \
                              jq -f {filter} -cr | gzip -c > {output}""",
                          input=self.input().path, filter=self.assets('73/filter.jq'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(self.path(ext='ldj.gz'), format=Gzip)


class MarburgExport(MarburgTask):
    """
    Attach and export.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='solr5vu3')

    def requires(self):
        return {
            'data': MarburgIntermediateSchema(date=self.date),
            'config': AMSLFilterConfig(date=self.date),
        }

    def run(self):
        output = shellout("span-tag -c {config} <(gunzip -c {input}) | span-export -o {format} > {output}",
                          config=self.input().get("config").path, input=self.input().get("data").path,
                          format=self.format)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(self.path(ext='ldj'))


class MarburgCombineNext(MarburgTask):
    """
    Harvest and combine into a single file.

    Adjust format, refs #5486.
    """

    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='nlm')

    def run(self):
        endpoint = "http://archiv.ub.uni-marburg.de/ubfind/OAI/Server"
        output = shellout("oaicrawl -w 8 -verbose -f oai_dc {endpoint} > {output}", endpoint=endpoint)
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout("echo '<records>' >> {output}", output=stopover)
        shellout("xmlcutty -path /OAI-PMH/GetRecord/record < {input} >> {output}", input=output, output=stopover)
        shellout("echo '</records>' >> {output}", output=stopover)
        luigi.LocalTarget(stopover).move(self.output().path)
        # Remove raw data.
        os.remove(output)

    def output(self):
        return luigi.LocalTarget(self.path())
