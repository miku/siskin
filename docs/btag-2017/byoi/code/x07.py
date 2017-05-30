#!/usr/bin/env python
# coding: utf-8

"""
TODO: MAG graph (or: crossref event data).
"""

import collections
import json

import luigi
from luigi.format import Gzip

from x06 import IntermediateSchema


class ReferenceList(luigi.ExternalTask):

    def output(self):
        return luigi.LocalTarget('inputs/mag.tsv.gz', format=Gzip)


class AddReferences(luigi.Task):

    def requires(self):
        return {
            'records': IntermediateSchema(),
            'refs': ReferenceList(),
        }

    def run(self):
        refmap = collections.defaultdict(list)

        with self.input().get('refs').open() as handle:
            for line in handle:
                parts = line.strip().split('\t')
                if len(parts) == 0:
                    continue
                doi, refs = parts[0], parts[1:]
                refmap[doi] = refs

        with self.input().get('records').open() as handle:
            with self.output().open('w') as output:
                for line in handle:
                    doc = json.loads(line)
                    try:
                        doc['refs'] = refmap[doc.get('doi')]
                    except KeyError:
                        pass
                    output.write(json.dumps(doc) + '\n')

    def output(self):
        return luigi.LocalTarget('outputs/combined-with-refs.is.ldj')

if __name__ == '__main__':
    luigi.run(local_scheduler=True)
