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
Fuzzy deduplication related tasks.
"""

from elasticsearch import helpers as eshelpers
from siskin.benchmark import timed
from gluish.common import Executable
from gluish.format import TSV
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
from siskin.utils import ElasticsearchMixin
import datetime
import elasticsearch
import luigi
import marcx
import tempfile

class FuzzyTask(DefaultTask):
    """ Fuzzy deduplication related things. """
    TAG = 'fuzzy'

    def closest(self):
        return weekly(self.date)

class FuzzyEditionList(FuzzyTask, ElasticsearchMixin):
    """ For a single index, create a TSV of the form

        Index, Id, Edition Statement

    where Edition Statement (250.a) is either NULL, a single string of multiple
    strings, separeted by a pipe (|).

    Index parameters can be adjusted like (space separated values in a single string):

        $ taskdo FuzzyEditionList --index ebl
    """
    date = ClosestDateParameter(default=datetime.date.today())
    index = luigi.Parameter(description='name of the index')

    @timed
    def run(self):
        es = elasticsearch.Elasticsearch([dict(host=self.es_host, port=self.es_port)])
        hits = eshelpers.scan(es, {'query': {'match_all': {}},
            'fields': ["content.250.a"]}, index=self.index,
            doc_type='title', scroll='10m', size=10000)
        with self.output().open('w') as output:
            for hit in hits:
                fields = hit.get('fields')
                if fields:
                    edition = 'NULL'
                    entries = fields.get('content.250.a', [])
                    if entries:
                        edition = '|'.join(entries).encode('utf-8')
                    output.write_tsv(hit['_id'], hit['_index'], edition)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class FuzzyEditionListRange(FuzzyTask, ElasticsearchMixin):
    """
    For a list of indices, create a FuzzyEditionList and concatenate them.

        $ taskdo FuzzyEditionList --indices "bsz ebl nep"
    """
    date = ClosestDateParameter(default=datetime.date.today())
    indices = luigi.Parameter(default="bsz ebl nep")

    def requires(self):
        indices = self.indices.split()
        for index in indices:
            yield FuzzyEditionList(date=self.date, index=index,
                                   es_host=self.es_host, es_port=self.es_port)

    @timed
    def run(self):
        _, output = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path, output=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True), format=TSV)

class FuzzyFieldList(FuzzyTask, ElasticsearchMixin):
    """ Run estab to gather a TSV of (id, index, type, values...) for  comparisons. """
    date = ClosestDateParameter(default=datetime.date.today())
    indices = luigi.Parameter(description='indices to extract values from')
    fields = luigi.Parameter(default='_id _index _type content.245.a content.245.b', description='values to extract')

    def requires(self):
        return Executable(name='estab', message='http://git.io/bLY7cQ')

    @timed
    def run(self):
        output = shellout("""estab -host {host} -port {port} -indices "{indices}" -f "{fields}" > {output} """,
                          indices=self.indices,fields=self.fields, host=self.es_host, port=self.es_port)
        luigi.File(output).move(self.output().path)

    def output(self):
            return luigi.LocalTarget(path=self.path(digest=True), format=TSV)

class FuzzyTitlePool(FuzzyTask, ElasticsearchMixin):
    """ Deduplicate titles. """

    date = ClosestDateParameter(default=datetime.date.today())
    source = luigi.Parameter(description='indices that are compared')
    target = luigi.Parameter(description='indices that are compared against')
    null = luigi.Parameter(default="NOT_AVAILABLE", significant=False)

    def requires(self):
        return {'file': FuzzyFieldList(date=self.date, indices=self.source, fields='_id _index _type content.245.a content.245.b'),
                'esmlt': Executable(name='esmlt', message='http://git.io/ckXUgA')}

    @timed
    def run(self):
        # find similar titles
        output = shellout("""esmlt -host {host} -port {port} -indices "{target}" -fields "content.245.a content.245.b"
                             -file "{file}" -columns "4,5" > {output} """, host=self.es_host, port=self.es_port,
                             file=self.input().get('file').path, target=self.target)
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with luigi.File(output, format=TSV).open() as handle:
            with luigi.File(stopover, format=TSV).open('w') as output:
                for row in handle.iter_tsv(cols=('idl', 'il', 'tl', 'til', 'sl', 'idr', 'ir', 'tr', 'score', 'tir', 'sr')):
                    ml = ' '.join([v for v in (row.til, row.sl) if v and not v == "NOT_AVAILABLE"])
                    mr = ' '.join([v for v in (row.tir, row.sr) if v and not v == "NOT_AVAILABLE"])
                    if not ml: ml = "NOT_AVAILABLE"
                    if not mr: mr = "NOT_AVAILABLE"
                    output.write_tsv(row.idl, row.il, row.tl, ml,
                                     row.idr, row.ir, row.tr, mr, row.score)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True), format=TSV)

class FuzzyCandidates(FuzzyTask):
    date = ClosestDateParameter(default=datetime.date.today())
    source = luigi.Parameter(description='indices that are compared')
    target = luigi.Parameter(description='indices that are compared against')
    null = luigi.Parameter(default="NOT_AVAILABLE", significant=False)

    measure = luigi.Parameter(default='ngram')
    threshold = luigi.FloatParameter(default=0.75)

    def requires(self):
        return {'file': FuzzyTitlePool(source=self.source, target=self.target, null=self.null, date=self.date),
                'stardust': Executable(name='stardust', message='http://git.io/_ehcwQ')}

    @timed
    def run(self):
        output = shellout("""stardust -f "4,8" {measure} {input} > {output}""", measure=self.measure, input=self.input().get('file').path)
        output = shellout(r"""/bin/bash -c "LANG=C sort -t$'\t' -k10,10 -nr {input} > {output}" """, input=output)
        output = shellout("""awk -F'\\t' '$10 > {threshold} {{print $0}}' {input} > {output} """, threshold=self.threshold, input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True), format=TSV)
