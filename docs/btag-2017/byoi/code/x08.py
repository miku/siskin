#!/usr/bin/env python
# coding: utf-8

"""
TODO: Licensing (KBART).
"""

import json
import tempfile

import luigi

from gluish.utils import shellout
from x06 import IntermediateSchema


class CreateConfig(luigi.Task):

    def run(self):
        """ Comes from amsl.technology """
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

    def requires(self):
        return {
            'config': CreateConfig(),
            'records': IntermediateSchema(),
        }

    def run(self):
        output = shellout(""" span-tag -c {config} {input} > {output} """,
                          config=self.input().get('config').path,
                          input=self.input().get('records').path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path='outputs/tagged.is.ldj')

if __name__ == '__main__':
    luigi.run(local_scheduler=True)
