#!/usr/bin/env python
# coding: utf-8

"""
Merging outputs
===============

Goals:

* show that tasks can require multiple things

"""

import tempfile

import luigi
from luigi.format import Gzip

from gluish.utils import shellout
from x04 import CrossrefIntermediateSchema, DOAJIntermediateSchema
from x05 import ArxivIntermediateSchema


def merge(targets):
    """
    Helper function to concatenate the outputs for a number of targets.
    """
    _, tf = tempfile.mkstemp(prefix='lab-')
    for target in targets:
        shellout("cat {input} >> {output}", input=target.path, output=tf)
    return tf


class IntermediateSchema(luigi.Task):

    def requires(self):
        """
        TODO: This tasks depends on three other tasks: the intermediate schemas
        of Crossref, DOAJ, Arxiv.
        """

    def run(self):
        merged = merge(self.input())
        luigi.LocalTarget(merged).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path='outputs/combined.is.ldj.gz', format=Gzip)

if __name__ == '__main__':
    luigi.run(local_scheduler=True)
