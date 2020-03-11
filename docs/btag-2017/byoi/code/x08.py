#!/usr/bin/env python
# coding: utf-8

"""
Licensing terms
===============

Goals:

* detour to the (complex) world of licensing
* example for managing licensing information and it's integration in the process

"""

import json
import logging
import os
import tempfile

import luigi
from gluish.utils import shellout
from luigi.format import Gzip
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
                    'or': [
                        {
                            'holdings': {
                                'file': 'inputs/de-15.tsv'
                            }
                        },
                        {
                            'collection': ["Arxiv"],
                        },
                    ]
                },
                'DE-14': {
                    'or': [
                        {
                            'holdings': {
                                'file': 'inputs/de-14.tsv'
                            }
                        },
                        {
                            'collection': ["Arxiv"],
                        },
                    ]
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
        """
        TODO: This task requires: CreateConfig, IntermediateSchema. We want to
        refer to these outputs by name, e.g. 'config' and 'records'.
        """

    def run(self):
        output = shellout(""" gunzip -c {input} | span-tag -c {config} | gzip -c > {output} """,
                          config=self.input().get('config').path,
                          input=self.input().get('records').path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path='outputs/tagged.is.ldj.gz', format=Gzip)

# Optional.
#
# Below we show an example of deduplication via DOI.
#
# Deduplication has many facets, groupcover is a simplistic tool that works with
# a certain CSV format.
#
#
# Here, we have a three-step process:
#
# 1. Extract a list of relevant information. A list of faster to process.
# 2. Use a tool (groupcover), that works on the extracted data and returns the changes to be made.
# 3. Apply these changed to the original data.


class ListifyRecords(luigi.Task):
    """
    Turn records in a list, which groupcover can understand.
    """

    def requires(self):
        """ TODO: Use intermediate schema file, that contains the licensing information. """

    def run(self):
        output = shellout("""gunzip -c {input} | jq -r '[
            .["finc.record_id"],
            .["finc.source_id"],
            .["doi"],
            .["x.labels"][]? ] | @csv' | sort -t, -k3 > {output}
        """, input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path='outputs/listified.csv')


class CalculateInstitutionChanges(luigi.Task):
    """
    Calculate institution changes based on DOI duplicates. Experimental, using
    https://github.com/miku/groupcover with preferences.
    """

    def requires(self):
        return ListifyRecords()

    def run(self):
        output = shellout("""groupcover -prefs '121 49 28' < {input} > {output}""",
                          input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path='outputs/institution-changes.tsv')


class TaggedAndDeduplicatedIntermediateSchema(luigi.Task):
    """
    A DOI deduplicated version of the intermediate schema. Experimental.
    """

    def requires(self):
        return {
            'changes': CalculateInstitutionChanges(),
            'file': TaggedIntermediateSchema(),
        }

    def run(self):
        """
        Keep a dictionary in memory with ids as keys and a list of ISILs as
        values.
        """
        updates = {}

        # First column is the ID, 4th to the end are the ISILs.
        output = shellout("cut -d, -f1,4- {changes} > {output}",
                          changes=self.input().get('changes').path)
        with open(output) as handle:
            for line in handle:
                fields = [s.strip() for s in line.split(',')]
                updates[fields[0]] = fields[1:]

        os.remove(output)
        logging.debug('%s changes staged', len(updates))

        # Weave in the changes.
        with self.input().get('file').open() as handle:
            with self.output().open('w') as output:
                for line in handle:
                    doc = json.loads(line)
                    identifier = doc["finc.record_id"]

                    if identifier in updates:
                        doc['x.labels'] = updates[identifier]

                    output.write(json.dumps(doc) + '\n')

    def output(self):
        return luigi.LocalTarget(path='outputs/tagged-deduplicated.is.ldj.gz', format=Gzip)


if __name__ == '__main__':
    luigi.run(local_scheduler=True)
