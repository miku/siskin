#!/usr/bin/env python
# coding: utf-8

"""
Merge sources.
"""


from __future__ import print_function

import tempfile

import luigi

from common import DefaultTask
from crossref import CrossrefIntermediateSchema
from doaj import DOAJIntermediateSchema
from gluish.utils import shellout
from oai import HarvestIntermediateSchema


class MergedTask(DefaultTask):
    TAG = 'merged'


class MergedSources(MergedTask):
    """ Merge various intermediate format files. """

    def requires(self):
        return [
            CrossrefIntermediateSchema(),
            DOAJIntermediateSchema(),
            HarvestIntermediateSchema(),
        ]

    def run(self):
        _, temp = tempfile.mkstemp(prefix='btag-')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path, output=temp)
        luigi.LocalTarget(temp).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

if __name__ == '__main__':
    luigi.run(['MergedSources', '--workers', '4', '--local-scheduler'])
