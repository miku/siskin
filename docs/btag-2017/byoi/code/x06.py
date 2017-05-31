#!/usr/bin/env python
# coding: utf-8

"""
TODO: Merging and deduplication (groupcover).
"""

import tempfile

import luigi

from luigi.format import Gzip
from gluish.utils import shellout
from x04 import CrossrefIntermediateSchema, DOAJIntermediateSchema
from x05 import ArxivIntermediateSchema


def merge(targets):
    """ Given a number of targets, concatenate them into a temporary file. """
    _, tmpf = tempfile.mkstemp(prefix='lab-')
    for target in targets:
        shellout("""cat {input} >> {output}""", input=target.path, output=tmpf)
    return tmpf


class IntermediateSchema(luigi.Task):

    def requires(self):
        return [
            CrossrefIntermediateSchema(),
            DOAJIntermediateSchema(),
            ArxivIntermediateSchema(),
        ]

    def run(self):
        merged = merge(self.input())
        luigi.LocalTarget(merged).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path='outputs/combined.is.ldj.gz', format=Gzip)

if __name__ == '__main__':
    luigi.run(local_scheduler=True)
