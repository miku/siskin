#!/usr/bin/env python
# coding: utf-8

"""
Licensing terms
===============

Goals:

* a show detour to the (complex) world of licensing
* example for managing licensing information and it's integration in the process

"""

import json
import tempfile

import luigi

from luigi.format import Gzip
from gluish.utils import shellout
from x06 import IntermediateSchema


class CreateConfig(luigi.Task):
    """
    This data can come from an outside system, e.g. http://amsl.technology.
    Need to express various licensing modes (e.g. KBART holding files, sources,
    collections, ISSN or subject lists) - more about this implementation at
    https://git.io/vH03I.
    """

    def run(self):
        with self.output().open('w') as output:
            config = {
                'DE-15': {
                    'holdings': {
                        'file': 'inputs/de-15.tsv'
                    }
                },
                'DE-14': {
                    'holdings': {
                        'file': 'inputs/de-14.tsv'
                    }
                }
            }
            output.write(json.dumps(config))

    def output(self):
        return luigi.LocalTarget(path='outputs/filterconfig.json')


class TaggedIntermediateSchema(luigi.Task):
    """
    We *tag* our normalized format here. Basically, we inject a new field into
    each document, which records, which library wants or is allowed to see a
    record.
    """

    def requires(self):
        return {
            'config': CreateConfig(),
            'records': IntermediateSchema(),
        }

    def run(self):
        output = shellout(""" gunzip -c {input} | span-tag -c {config} | gzip -c > {output} """,
                          config=self.input().get('config').path,
                          input=self.input().get('records').path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path='outputs/tagged.is.ldj.gz', format=Gzip)


if __name__ == '__main__':
    luigi.run(local_scheduler=True)
