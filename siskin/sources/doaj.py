# coding: utf-8
# pylint: disable=F0401,W0232,E1101,C0103,C0301,W0223,E1123,R0904,E1103

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
Directory of Open Access Journals.

DOAJ is an online directory that indexes and provides access to high quality,
open access, peer-reviewed journals.

http://doaj.org
"""

import datetime
import itertools
import operator
import os
import tempfile
import time
from builtins import map, range

import elasticsearch
import luigi
import ujson as json

from gluish.common import Executable
from gluish.format import TSV, Gzip
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.benchmark import timed
from siskin.task import DefaultTask
from siskin.utils import load_set_from_file, load_set_from_target


class DOAJTask(DefaultTask):
    """
    Base task for DOAJ.
    """
    TAG = '28'

    def closest(self):
        return monthly(date=self.date)


class DOAJHarvest(DOAJTask):
    """
    Harvest via OAI endpoint.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout(
            "METHA_DIR={dir} metha-sync {endpoint} && METHA_DIR={dir} metha-cat {endpoint} | pigz -c > {output}",
            dir=self.config.get('core', 'metha-dir'),
            endpoint='http://www.doaj.org/oai.article')
        self.logger.debug("compressing output (might take a few minutes) ...")
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml.gz'))


class DOAJIntermediateSchemaDirty(DOAJTask):
    """
    Convert to intermediate schema via span; elasticsearch and API are
    currently defunkt, use OAI (with metha 0.1.37 or higher).

    Dirty version, contains dups (like https://is.gd/AJ0qKc).

    Example duplicates:

    * https://doaj.org/article/697e7128445f406fa108acf55a3d0b52
    * https://doaj.org/article/fb7aa986929e4644b6125214bade736f

    Example seems to differ only in abstract formatting (https://i.imgur.com/ILxmRVC.png), with
    fb7aa986929e4644b6125214bade736f probably being correct (also the newest
    record).

    2011-01-01T00:00:00Z    5a29b0fd51bf458eaa173f5c40f3d21c        A CLINICAL EXPERIENCE OF METHOTREXATE USE IN TREATMENT OF PATIENT WITH JUVENILE OLIGOARTHRITIS
    2011-01-01T00:00:00Z    697e7128445f406fa108acf55a3d0b52        A CLINICAL EXPERIENCE OF METHOTREXATE USE IN TREATMENT OF PATIENT WITH JUVENILE OLIGOARTHRITIS
    2011-01-01T00:00:00Z    fb7aa986929e4644b6125214bade736f        A CLINICAL EXPERIENCE OF METHOTREXATE USE IN TREATMENT OF PATIENT WITH JUVENILE OLIGOARTHRITIS
    2011-11-01T00:00:00Z    fb7aa986929e4644b6125214bade736f        A CLINICAL EXPERIENCE OF METHOTREXATE USE IN TREATMENT OF PATIENT WITH JUVENILE OLIGOARTHRITIS

    Another example (DOAJ internal search seems to return both: https://is.gd/AJ0qKc):

    2006-09-01T00:00:00Z    e211856315f442869421032269b461d6        11 de setembro de 2001: algumas das consequências após 5 anos.
    2010-12-01T00:00:00Z    d8e93099d0934e6eb3f4f66e8e522a9c        11 de setembro de 2001: algumas das consequências após 5 anos.

    """
    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default="doaj-oai",
                             description="kind of source document, doaj-oai (defunkt: doaj, doaj-api)")

    def requires(self):
        return {
            'span-import': Executable(name='span-import', message='http://git.io/vI8NV'),
            'input': DOAJHarvest(date=self.date),
        }

    @timed
    def run(self):
        output = shellout("""unpigz -c {input} |
                             span-import -i {format} |
                             grep -vf {exclude} |
                             pigz -c > {output}""",
                          exclude=self.assets('028_doaj_filter_issn.tsv'),
                          input=self.input().get('input').path,
                          format=self.format)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))


class DOAJTable(DOAJTask):
    """
    Emit a DOAJ data table: id, date, title - for rough deduplication by title (see DOAJWhitelist).
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return DOAJIntermediateSchema(date=self.date)

    def run(self):
        output = shellout(
            """unpigz -c {input} | jq -r '[.["finc.record_id"], .["x.date"], .["rft.atitle"]] | @tsv' > {output} """,
            input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class DOAJWhitelist(DOAJTask):
    """
    Create a list of suppressable DOAJ ids.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return DOAJTable(date=self.date)

    def run(self):
        # Sort by title, then date, the find the newest date and filter the id.
        output = shellout(""" sort -S30% -t $'\t' -k3,3 -k2,2 < {input} |
                          tac |
                          sort -S30% -t $'\t' -k3,3 -u |
                          cut -f1 | grep -v '^$' > {output} """,
                          input=self.input().path,
                          preserve_whitespace=True)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class DOAJIntermediateSchema(DOAJTask):
    """
    Respect whitelist.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default="doaj-oai",
                             description="kind of source document, doaj-oai (defunkt: doaj, doaj-api)")

    def requires(self):
        return {
            'data': DOAJIntermediateSchemaDirty(date=self.date, format=self.format),
            'whitelist': DOAJWhitelist(date=self.date),
        }

    @timed
    def run(self):
        output = shellout("""unpigz -c {input} | LC_ALL=C grep -Ff {whitelist} | pigz -c > {output}""",
                          whitelist=self.input().get('whitelist').path,
                          input=self.input().get('data').path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))


class DOAJISSNList(DOAJTask):
    """
    A list of DOAJ ISSNs.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'input': DOAJIntermediateSchema(date=self.date),
            'jq': Executable(name='jq', message='http://git.io/NYpfTw')
        }

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        shellout("""jq -r '.["rft.issn"][]?' <(unpigz -c {input}) >> {output} """,
                 input=self.input().get('input').path,
                 output=stopover)
        shellout("""jq -r '.["rft.eissn"][]?' <(unpigz -c {input}) >> {output} """,
                 input=self.input().get('input').path,
                 output=stopover)
        output = shellout("""sort -u {input} > {output} """, input=stopover)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class DOAJDOIList(DOAJTask):
    """
    An best-effort list of DOAJ DOIs.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'input': DOAJIntermediateSchema(date=self.date),
            'jq': Executable(name='jq', message='http://git.io/NYpfTw'),
        }

    @timed
    def run(self):
        output = shellout(
            """jq -r '.doi' <(unpigz -c {input}) | grep -v "null" | grep -o "10.*" 2> /dev/null | sort -u > {output} """,
            input=self.input().get('input').path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class DOAJDownloadDump(DOAJTask):
    """
    Download complete set.

    > You can access full data-dumps of the entire DOAJ public metadata below.
    Data dumps are generated weekly. The following files provide you complete
    sets of articles and journals as JSON, in the form that they would also be
    retrieved via the API.

    Data comes in batches of 100k records.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    kind = luigi.Parameter(default='article', description='article or journal')

    def run(self):
        dumps = {
            'article': 'https://doaj.org/public-data-dump/article',
            'journal': 'https://doaj.org/public-data-dump/journal',
        }
        if self.kind not in dumps:
            raise ValueError('kind must be: article or journal')

        original = shellout(""" wget -O {output} {link} """, link=dumps[self.kind])

        # The data comes in batches, each batch is a list of dicts, [{}, {},
        # ...], so we want to flatten all batches into a single json lines file first.
        output = shellout(""" tar -xOzf {input} | jq -rc '.[]' | pigz -c > {output} """, input=original)
        luigi.LocalTarget(output).move(self.output().path)
        os.remove(original)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='gz'), format=Gzip)
