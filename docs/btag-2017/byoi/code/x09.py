#!/usr/bin/env python
# coding: utf-8

"""
Convert to SOLR schema.
"""

import luigi

from gluish.utils import shellout
from x08 import TaggedIntermediateSchema


class Export(luigi.Task):

    format = luigi.Parameter(default='solr5vu3')

    def requires(self):
        return TaggedIntermediateSchema()

    def run(self):
        output = shellout(""" gunzip -c {input} | span-export -o {format} | gzip -c > {output} """,
                          format=self.format, input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path='outputs/export-%s.ldj.gz' % self.format)

if __name__ == '__main__':
    luigi.run(local_scheduler=True)
