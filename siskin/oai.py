# coding: utf-8
# pylint: disable=C0301,C0103,E1101

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
Experimental tasks for generic OAI sources. Works fine, if data source is somewhat standard-conform.

On the command line:

    $ taskdo OAIIntermediateSchema --url http://example.com/oai --prefix oai_dc --sid 123 --collection '"Stellar Articles"' --format ElectronicArticle

Other OAI considerations:

Decouple harvesting, extend metha, so it can harvest endpoints on autopilot,
place files into a special folder and configure this folder in siskin. Then
just require the already prepared file:

    def requires(self):
        return IntermediateSchemaFromOAI(endpoint="...", format="...")


"""

import datetime

import luigi

from gluish.common import Executable
from gluish.utils import shellout
from siskin.task import DefaultTask


class OAIBaseTask(DefaultTask):
    """
    Base task for generic OAI tasks.
    """
    TAG = "oai"


class OAICombine(OAIBaseTask):
    """
    Combine harvested files for a data source into a single file.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://export.arxiv.org/oai2", significant=False)
    prefix = luigi.Parameter(default="oai_dc", significant=False)

    def requires(self):
        return [
            Executable(name='metha-sync', message='https://github.com/miku/metha'),
            Executable(name='pigz', message='http://zlib.net/pigz/'),
        ]

    def run(self):
        shellout("METHA_DIR={dir} metha-sync -format {prefix} {url}", prefix=self.prefix, url=self.url, dir=self.config.get('core', 'metha-dir'))
        output = shellout("METHA_DIR={dir} metha-cat -root Records -format {prefix} {url} | pigz -c > {output}",
                          prefix=self.prefix, url=self.url, dir=self.config.get('core', 'metha-dir'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="xml.gz"))


class OAIIntermediateSchema(OAIBaseTask):
    """
    Convert OAI via flux, pass SID and collection as arguments.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    url = luigi.Parameter(significant=False)
    prefix = luigi.Parameter(significant=False)

    sid = luigi.Parameter(description="internal data source id")
    collection = luigi.Parameter(description="internal collection name")
    format = luigi.Parameter(default='ElectronicArticle')

    def requires(self):
        return OAICombine(date=self.date, url=self.url, prefix=self.prefix)

    def run(self):
        mapdir = 'file:///%s' % self.assets("maps/")
        output = shellout("""flux.sh {flux}
                             MAP_DIR={mapdir}
                             in={input}
                             sid={sid}
                             mega_collection={collection}
                             format={format} | pigz -c > {output}""",
                          flux=self.assets("oai/flux.flux"), mapdir=mapdir, sid=self.sid,
                          collection=self.collection, format=self.format, input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz', digest=True))
