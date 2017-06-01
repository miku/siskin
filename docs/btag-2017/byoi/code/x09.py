#!/usr/bin/env python
# coding: utf-8

"""
Final conversion to SOLR format
===============================

Goals:

* a task parameter
* parameterize output filename to reflect task parameters (see also: https://git.io/vH0sO)

"""

import luigi

from gluish.utils import shellout
from x08 import TaggedIntermediateSchema


class Export(luigi.Task):
    """
    This task uses a parameter. There are different parameter types like
    IntParameter, DateParameter, ...
    """
    format = luigi.Parameter(default='solr5vu3', description='solr5vu3 or formeta')

    def requires(self):
        return TaggedIntermediateSchema()

    def run(self):
        output = shellout("gunzip -c {input} | span-export -o {format} | gzip -c > {output} ",
                          format=self.format, input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path='outputs/export-%s.ldj.gz' % self.format)

if __name__ == '__main__':
    luigi.run(local_scheduler=True)
