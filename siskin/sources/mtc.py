# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301
#
#  Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                 by The Finc Authors, http://finc.info
#                 by Martin Czygan, <martin.czygan@uni-leipzig.de>
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
#

"""
Music Treasury Collection.
"""

from gluish.format import TSV
from gluish.intervals import quarterly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from luigi.contrib.esindex import CopyToIndex
from lxml import etree
from siskin.benchmark import timed
from siskin.task import DefaultTask
from siskin.utils import iterfiles, wc
import BeautifulSoup
import cStringIO
import datetime
import luigi
import os
import tempfile

class MTCTask(DefaultTask):
    TAG = '010'

    def closest(self):
        """ Try redownload every three month. """
        return quarterly(date=self.date)

class MTCSitemap(MTCTask):
    """ Download the sitemap. """
    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default='http://lcweb2.loc.gov/diglib/ihas/consortium2/sitemap.xml', significant=False)

    @timed
    def run(self):
        output = shellout("wget -q --retry-connrefused -O {output} {url}", url=self.url)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))

class MTCStylesheet(MTCTask):
    """ Download MODS -> MARC21 XSL stylesheet. TODO: this could be added to
    the assets. """
    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default='http://www.loc.gov/standards/marcxml/xslt/MODS2MARC21slim.xsl', significant=False)

    @timed
    def run(self):
        output = shellout("""wget -q --retry-connrefused -O {output} {url}""", url=self.url)
        output = shellout(""" sed -e 's@include href="http://www.loc.gov/marcxml/xslt/MARC21slimUtils.xsl"@include href="{local}"@' {input} > {output}""", input=output, local=self.assets("MARC21slimUtils.xsl"))
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xsl'))

class MTCUrls(MTCTask):
    """ Extract URLs from MTC sitemap. The sitemap just contains links
    to HTML files, but the path to the metadata is almost the same.

    #TODO: maybe via XSLT, too?
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return MTCSitemap(date=self.date)

    @timed
    def run(self):
        with self.input().open('r') as handle:
            soup = BeautifulSoup.BeautifulStoneSoup(handle.read())
            locs = soup.findAll('loc')
            with self.output().open('w') as output:
                for loc in locs:
                    url = loc.text.replace('default.html', 'mods.xml')
                    if url.endswith('.xml'):
                        output.write_tsv(url)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class MTCModsImport(MTCTask):
    """ Download all files sequentially. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return MTCUrls(date=self.date)

    @timed
    def run(self):
        # create target subdirectory
        target = os.path.join(os.path.dirname(self.output().path), str(self.closest()))
        if not os.path.exists(target):
            os.makedirs(target)
        size = wc(self.input().path)

        with self.input().open() as handle:
            for i, row in enumerate(handle.iter_tsv(cols=('url',)), start=1):
                name = os.path.join(target, row.url.split('/')[-2])
                destination = "{name}.xml".format(name=name)
                if not os.path.exists(destination):
                    output = shellout("""wget -q --retry-connrefused
                                      {url} -O {output}""", url=row.url)
                    luigi.File(output).move(destination)
                self.logger.debug("{0}/{1} {2}".format(i, size, row.url))

        # write "receipt"
        with self.output().open('w') as output:
            for path in iterfiles(target):
                if path.endswith('.xml'):
                    output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)

class MTCMarcXML(MTCTask):
    """ Convert the .mods XML files to Marc XML files via `xsltproc`.
    Again, the completion is indicated by a file containing all pathes
    to the converted records.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'stylesheet': MTCStylesheet(date=self.date),
                'filelist': MTCModsImport(date=self.date)}

    @timed
    def run(self):
        target = os.path.join(os.path.dirname(self.output().path), str(self.date))
        if not os.path.exists(target):
            os.makedirs(target)

        _, errorlog = tempfile.mkstemp(prefix='siskin-')
        stylesheet = self.input().get('stylesheet').path
        size = wc(self.input().get('filelist').path)

        with self.input().get('filelist').open() as handle:
            for i, row in enumerate(handle.iter_tsv(cols=('path',)), start=1):
                basename = os.path.basename(row.path)
                name = basename.replace(".xml", ".marcxml")
                destination = os.path.join(target, name)
                if not os.path.exists(destination):
                    try:
                        output = shellout("xsltproc {xsl} {input} > {output}",
                                          input=row.path, xsl=stylesheet)
                        luigi.File(output).move(destination)
                    except RuntimeError as err:
                        self.logger.error("{0}: {1}".format(row.path, err))
                        with open(errorlog, 'a') as log:
                            log.write('%s\t%s\n' % (row.path, err))
                self.logger.debug("{0}/{1} {2}".format(i, size, row.path))

        # write receipt
        with self.output().open('w') as output:
            for path in iterfiles(target):
                output.write_tsv(path)

        # this is just a temporary artefact for now
        self.logger.debug("Conversion errors logged at: {0}".format(errorlog))

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)

class MTCCombine(MTCTask):
    """ Convert Marc XML to Marc8, via `yaz-marcdump`.
    yaz-marcdump version 4.X returned 0 to the shell on yaz_marc_read_xml failed,
    whereas version 5.X exits with a non-zero code (5).
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return MTCMarcXML(date=self.date)

    @timed
    def run(self):
        target = os.path.join(os.path.dirname(self.output().path), str(self.date))
        if not os.path.exists(target):
            os.makedirs(target)

        size = wc(self.input().path)
        _, combined = tempfile.mkstemp(prefix='siskin-')

        with self.input().open() as handle:
            for i, row in enumerate(handle.iter_tsv(cols=('path',)), start=1):

                # Cleanup wrongly nested data fields, see:
                # https://gist.github.com/miku/ea779a221d00b5524fcd
                # in 2014-05, this corrects 673 errors, while 31 are not yet
                # recoverable!
                with open(row.path) as handle:
                    f = cStringIO.StringIO(handle.read())
                    doc = etree.parse(f)

                result = doc.xpath('/marc:record/marc:datafield/marc:datafield',
                          namespaces={'marc': 'http://www.loc.gov/MARC21/slim'})
                if len(result) > 0:
                    self.logger.debug("Fixing broken MARCXML in: {0}".format(row.path))
                for misplaced in result:
                    parent = misplaced.getparent()
                    record = misplaced.getparent().getparent()
                    parent.remove(misplaced)
                    record.append(misplaced)

                _, cleaned = tempfile.mkstemp(prefix='siskin-')
                with open(cleaned, 'w') as output:
                    output.write(etree.tostring(doc, pretty_print=True))

                # actually do the conversion ...
                basename = os.path.basename(row.path)
                name = basename.replace(".marcxml", ".mrc")
                destination = os.path.join(target, name)
                if not os.path.exists(destination):
                    # exit(5) for serious decoding errors
                    # see: http://www.indexdata.com/yaz/doc/NEWS
                    shellout("""yaz-marcdump -i marcxml -o marc
                             {input} >> {output}""", input=cleaned,
                             output=combined, ignoremap={5: 'FIXME'})
                self.logger.debug("{0}/{1} {2}".format(i, size, row.path))

        luigi.File(combined).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class MTCJson(MTCTask):
    """ Convert XML files to a single JSON file."""
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return MTCCombine(date=self.date)

    def run(self):
        output = shellout("marctojson -m date={date} {input} > {output}",
                       date=self.closest(), input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class MTCIndex(MTCTask, CopyToIndex):
    date = ClosestDateParameter(default=datetime.date.today())

    index = 'mtc'
    doc_type = 'title'
    purge_existing_index = True

    mapping = {'title': {'date_detection': False,
                          '_id': {'path': 'content.001'},
                          '_all': {'enabled': True,
                                   'term_vector': 'with_positions_offsets',
                                   'store': True}}}

    def update_id(self):
        """ This id will be a unique identifier for this indexing task."""
        return self.effective_task_id()

    def requires(self):
        return MTCJson(date=self.date)
