# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

"""
HMT related workflows.

Data reconciliation, with target artefacts and precision reports.
"""

from __future__ import print_function
from gluish.format import TSV
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.sources.naxos import NaxosJson
from siskin.task import DefaultTask
from siskin.utils import ElasticsearchMixin
import datetime
import itertools
import json
import luigi


class HMTTask(DefaultTask):
    TAG = 'hmt'
    NULL = '<NULL>'

    def closest(self):
        return weekly(date=self.date)

class HMTNaxosPersonsAndDates(HMTTask):
    """
    Dump all people and their bio data from 100 and 700 fields.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    encoding = luigi.Parameter(default='utf-8', significant=False)

    def requires(self):
        return NaxosJson(date=self.date)

    def run(self):
        with self.input().open() as handle:
            with self.output().open('w') as output:
                for _, line in enumerate(handle):
                    c = json.loads(line).get('content')
                    id = c.get('001', self.NULL)
                    for field in itertools.chain(c.get('100', []), c.get('700', [])):
                        output.write_tsv(id, field.get('a').encode(self.encoding), field.get('d', self.NULL))

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class HMTNaxosUniqPersonsAndDates(HMTTask):
    """ Return the unique authors. """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return HMTNaxosPersonsAndDates(date=self.date)

    def run(self):
        output = shellout("cut -f 2-3 < {input} | LANG=C sort -u > {output}",
                          input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class HMTNaxosCompletePersonsAndDates(HMTTask):
    """
    Only names with dates.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return HMTNaxosUniqPersonsAndDates(date=self.date)

    def run(self):
        output = shellout("""LANG=C grep -v "{null}" {input} > {output}""",
                          null=self.NULL, input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class HMTNaxosIncompleteAuthorsAndDates(HMTTask):
    """
    Only names without dates.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return HMTNaxosUniqPersonsAndDates(date=self.date)

    def run(self):
        output = shellout("""LANG=C grep "{null}" {input} > {output}""",
                          null=self.NULL, input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
