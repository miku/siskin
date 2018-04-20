# coding: utf-8
# pylint: disable=C0301,E1101

# Copyright 2018 by Leipzig University Library, http://ub.uni-leipzig.de
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
Disson, Dissertations Online, refs #3422.
"""

import collections
import datetime
import json
import re

import luigi
import pymarc
import marcx
import tqdm

from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask


class DissonTask(DefaultTask):
    """
    Base task for disson.
    """
    TAG = '13'

    def closest(self):
        return monthly(date=self.date)


class DissonHarvest(DissonTask):
    """
    Via OAI, access might be restricted.

    ;; ANSWER SECTION:
    services.dnb.de.	3539	IN	CNAME	httpd-e02.dnb.de.
    httpd-e02.dnb.de.	3539	IN	CNAME	prodche-02.dnb.de.
    prodche-02.dnb.de.	3539	IN	A	    193.175.100.222

    Format options (2018-03-02):

    * baseDc-xml
    * BibframeRDFxml
    * JATS-xml
    * MARC21plus-1-xml
    * MARC21plus-xml
    * MARC21-xml
    * mods-xml
    * oai_dc
    * ONIX-xml
    * PicaPlus-xml
    * RDFxml
    * sync-repo-xml
    * xMetaDiss

    """
    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='MARC21-xml')
    set = luigi.Parameter(default='dnb-all:online:dissertations')

    def run(self):
        endpoint = "http://services.dnb.de/oai/repository"
        shellout("metha-sync -daily -format {format} -set {set} {endpoint}",
                 format=self.format, set=self.set, endpoint=endpoint)
        output = shellout("""metha-cat -format {format} -set {set} {endpoint} |
                             pigz -c > {output}""", format=self.format, set=self.set,
                          endpoint=endpoint)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml.gz', digest=True))


class DissonMARC(DissonTask):
    """
    Create a binary MARC version.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return DissonHarvest(date=self.date)

    def run(self):
        output = shellout("yaz-marcdump -i marcxml -o marc <(unpigz -c {input}) > {output}",
                          input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))


class DissonIntermediateSchema(DissonTask):
    """
    Convert MARC to intermediate schema.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return DissonMARC(date=self.date)

    def run(self):

        stats = collections.Counter()

        def parse_date(record):
            """
            XXX: Factor this out.
            """
            v = rr.firstvalue('264.c')
            if not v:
                v = rr.firstvalue('502.a')
                if not v:
                    v = rr["005"].value()[:4]
                    if not v:
                        raise RuntimeError('unparsed date (no 264.a, 502.a, 005):\n%s' % record)
                    stats['005'] += 1
                else:
                    stats['502.a'] += 1

                match = re.search(r'.*(\d\d\d\d).*', v)
                if match:
                    v = match.group(1)
                else:
                    raise RuntimeError("ignoring broken (%s) record: %s" % (v, rr["001"].value()))

            else:
                stats['264.c'] += 1

            v = re.sub("[^0-9]", "", v)
            if v and len(v) == 4:
                return '%s-01-01' % v

            raise RuntimeError('unparsed date %s: %s' % (v, record))

        with self.input().open() as handle:
            with self.output().open('w') as output:
                reader = pymarc.MARCReader(handle, to_unicode=True)
                for record in tqdm.tqdm(reader):
                    rr = marcx.Record().from_record(record)
                    identifier = rr['001'].value()

                    try:
                        date = parse_date(rr)
                    except RuntimeError as err:
                        self.logger.debug(u"%s" % err)
                        stats['ignored'] += 1
                        continue

                    doc = {
                        'finc.source_id': '13',
                        'finc.record_id': identifier,
                        'finc.id': 'ai-13-%s' % (identifier),
                        'rft.date': date,
                        'urls': list(rr.itervalues('856.u')),
                        'rft.btitle': rr.firstvalue('245.a'),
                        'subjects': list(rr.itervalues('689.a', '650.a')),
                        'authors': [{'rft.au': name} for name in rr.itervalues('100.a')],
                    }
                    output.write("%s\n" % json.dumps(doc))

        self.logger.debug(json.dumps(stats.most_common()))

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))
