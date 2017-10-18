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
DawsonEra, #9773, SID124.

Experimental: zip, marcxml, missing ns, illegal characters.

Configuration
-------------

[dawson]

download-url = http://exa.com/store/123

"""

import luigi
from gluish.utils import shellout

from siskin.task import DefaultTask


class DawsonTask(DefaultTask):
    """
    Base class for Dawson.
    """
    TAG = '124'


class DawsonDownload(DawsonTask):
    """
    Download.
    """

    def run(self):
        output = shellout("curl --fail -sL {url} > {output}",
                          url=self.config.get('dawson', 'download-url'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='zip'))


class DawsonFixAndCombine(DawsonTask):
    """
    Remove namespace, combine all files into a single file.

    > Exception in thread "main" org.culturegraph.mf.exceptions.MetafactureException:
    org.xml.sax.SAXParseException; lineNumber: 540690; columnNumber: 68; An invalid
    XML character (Unicode: 0x1a) was found in the element content of the document.
    """

    def requires(self):
        return DawsonDownload()

    def run(self):
        output = shellout(r"""
            echo '<?xml version="1.0" encoding="UTF-8"?>
            <collection xmlns="http://www.loc.gov/MARC21/slim">
            ' >> {output} &&

            unzip -p {input} |
            sed -e 's@<mx:collection xmlns="http://www.loc.gov/MARC21/slim">@@' |
            sed -e 's@<?xml version="1.0" encoding="UTF-8"?>@@' |
            sed -e 's@</mx:collection>@@' |
            sed -e 's@<mx:@<@g;s@</mx:@</@g' |
            tr -d '\032\033' >> {output} &&

            echo '</collection>' >> {output}""",
                          input=self.input().path, preserve_whitespace=True)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))


class DawsonIntermediateSchema(DawsonTask):
    """
    Convert to intermediate schema via metafacture. Custom morphs and flux are
    kept in assets/124. Maps are kept in assets/maps.
    """

    def requires(self):
        return DawsonFixAndCombine()

    def run(self):
        mapdir = 'file:///%s' % self.assets("maps/")
        output = shellout("""flux.sh {flux} in={input} MAP_DIR={mapdir} > {output}""",
                          flux=self.assets("124/124.flux"), mapdir=mapdir, input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))


class DawsonExport(DawsonTask):
    """
    Export via span-export. Hard-wired tag: DE-82.
    """
    format = luigi.Parameter(default='solr5vu3', description='export format, see span-export -list')

    def requires(self):
        return DawsonIntermediateSchema()

    def run(self):

        output = shellout("""
            cat {input} |
            span-tag -c '{{"DE-82": {{"any": {{}}}}}}' |
            span-export -o {format} > {output}""",
                          format=self.format, input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='fincsolr.ndj'))
