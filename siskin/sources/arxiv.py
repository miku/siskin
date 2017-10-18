# coding: utf-8
# pylint: disable=C0301,E1101

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
arxiv.org

http://arxiv.org/help/oa/index

----

Config:

[core]

metha-dir = /path/to/dir

----

From: https://groups.google.com/forum/#!topic/arxiv-api/aOacIt6KD2E

> the problem is that the API is not the appropriate vehicle to obtain a full
copy of arXiv metadata. It is intended for interactive queries and search
overlays and should not be used for full replication. The size of result sets
is capped at a large but finite number. You should use OAI-PMH instead for your
purposes.

"""

import datetime

import luigi
from gluish.common import Executable
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

from siskin.task import DefaultTask


class ArxivTask(DefaultTask):
    """
    Base task.
    """
    TAG = '121'

    def closest(self):
        return weekly(date=self.date)


class ArxivCombine(ArxivTask):
    """
    Single file dump.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://export.arxiv.org/oai2", significant=False)
    prefix = luigi.Parameter(default="oai_dc", significant=False)

    def requires(self):
        return Executable(name='metha-sync', message='https://github.com/miku/metha')

    def run(self):
        shellout("METHA_DIR={dir} metha-sync -format {prefix} {url}",
                 prefix=self.prefix, url=self.url, dir=self.config.get('core', 'metha-dir'))
        output = shellout("METHA_DIR={dir} metha-cat -root Records -format {prefix} {url} | pigz -c > {output}",
                          prefix=self.prefix, url=self.url, dir=self.config.get('core', 'metha-dir'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="xml.gz"))


class ArxivIntermediateSchema(ArxivTask):
    """
    Better, still experimental.

    Convert to intermediate schema via metafacture. Custom morphs and flux are
    kept in assets/arxiv. Maps are kept in assets/maps.

    Note: There are early arxiv XML records, that do not get an ID in the transformation, e.g.

    {
      "finc.mega_collection": "Arxiv",
      "finc.source_id": "arxiv",
      "rft.atitle": "A determinant of Stirling cycle numbers counts unlabeled acyclic   single-source automata",
      "abstract": "Comment: 11 pages",
      "rft.date": "2007-03-30",
      "x.date": "2007-03-30T00:00:00Z",
      "authors": [
        {
          "rft.au": "Callan, David"
        }
      ],
      "url": [
        "http://arxiv.org/abs/0704.0004"
      ],
      "finc.format": "text",
      "x.subjects": [
        "Mathematics - Combinatorics",
        "05A15"
      ]
    }

    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return ArxivCombine(date=self.date)

    def run(self):
        """
        With 3.1.1 -Xmx512m seemed to be enough. Also: java -Xmx1G -XX:+PrintFlagsFinal -Xmx2G 2>/dev/null | grep MaxHeapSize.
        """
        mapdir = 'file:///%s' % self.assets("maps/")
        output = shellout("""FLUX_JAVA_OPTIONS="-Xmx2048m" flux.sh {flux} in={input} MAP_DIR={mapdir} | pigz -c > {output}""",
                          flux=self.assets("arxiv/arxiv.flux"), mapdir=mapdir, input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))


class ArxivExport(ArxivTask):
    """
    Export to various formats
    """
    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='solr5vu3', description='export format')

    def requires(self):
        return ArxivIntermediateSchema(date=self.date)

    def run(self):
        output = shellout("span-export -o {format} <(unpigz -c {input}) | pigz -c > {output}",
                          format=self.format, input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        extensions = {
            'solr5vu3': 'ldj.gz',
            'formeta': 'form.gz',
        }
        return luigi.LocalTarget(path=self.path(ext=extensions.get(self.format, 'gz')))
