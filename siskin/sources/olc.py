# coding: utf-8
# pylint: disable=C0301,E1101

# Copyright 2019 by Leipzig University Library, http://ub.uni-leipzig.de
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
OLC, refs #16279.

A list of (51) collection names and internal identifiers can be found here:
https://is.gd/8DFvOo (GBV).
"""

import datetime
import glob
import json
import os

import luigi
from gluish.format import Gzip
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.conversions import olc_to_intermediate_schema
from siskin.task import DefaultTask
from siskin.utils import sha1obj


class OLCTask(DefaultTask):
    """
    OLC base task.
    """
    TAG = '68'

    # cf. https://is.gd/8DFvOo
    COLLECTIONS = [
        'SSG-OLC-ANG',
        'SSG-OLC-ARC',
        'SSG-OLC-BUB',
        'SSG-OLC-FTH',
        'SSG-OLC-GER',
        'SSG-OLC-GWK',
        'SSG-OLC-KPH',
        'SSG-OLC-MKW',
        'SSG-OLC-MUS',
        'SSG-OLC-PHI',
    ]

    def closest(self):
        return monthly(date=self.date)


class OLCDump(OLCTask):
    """
    Fetch a slice (or all of a SOLR), refs #16279.

    Currently, 9/51 collections are requested, but that might change over time.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        query = ' OR '.join(["collection_details:{}".format(c) for c in self.COLLECTIONS])
        output = shellout(""" solrdump -verbose -server {server} -q '{query}' | jq -rc . | pigz -c > {output} """,
                          query=query,
                          server=self.config.get('olc', 'solr'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        filename = '{}-{}.ndj.gz'.format(self.date, sha1obj(self.COLLECTIONS))
        return luigi.LocalTarget(path=self.path(filename=filename), format=Gzip)


class OLCIntermediateSchema(OLCTask):
    """
    Sample convertion.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return OLCDump(date=self.date)

    def run(self):
        with self.input().open() as file:
            with self.output().open('w') as output:
                for i, line in enumerate(file):
                    if i % 100000 == 0:
                        self.logger.debug("@{}".format(i))
                    doc = json.loads(line)
                    result = olc_to_intermediate_schema(doc)
                    output.write(bytes(json.dumps(result), encoding="utf-8"))
                    output.write(b"\n")

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ndj.gz'), format=Gzip)


class OLCIntermediateSchemaDeprecated(OLCTask):
    """
    Run and collect OLC, 68, standalone script. There is licensing information
    added in the standalone script and later during AILicening, too. That
    should be ok.
    """

    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        """
        Find the most recent output, compress and move into the "siskin" tree.
        """
        shellout("{script} --overwrite --outputformat json",
                 script=self.assets("68/68-fincjson", ignoremap={1: "most probably ok for skip, due to existing file"}))

        # Find the output file.
        taskdir = os.path.join(self.BASE, self.TAG)
        outputs = sorted(glob.glob(os.path.join(taskdir, '68-output-*json')), reverse=True)
        if len(outputs) == 0:
            raise RuntimeError("could not find any artifacts for source at {}".format(taskdir))
        path = outputs[0]

        # Compress as AIIntermediateSchema requires all artifacts to be gzip compressed.
        output = shellout("pigz -c {input} > {output}", input=path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ndj.gz'))
