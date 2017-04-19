#!/usr/bin/env python
# coding: utf-8

"""
Convert Crossref API JSON to intermediate format.

Either use a downloaded API dump or selectively query the API for e.g. ISSN found in KBART.

Showcase jq.
"""

from __future__ import print_function

import luigi

from common import DefaultTask
from gluish.utils import shellout


class CrossrefTask(DefaultTask):
    """ Crossref related tasks. """
    TAG = 'crossref'


class CrossrefSnippet(luigi.ExternalTask, CrossrefTask):
    """ Some batch API excerpt. """

    def output(self):
	return luigi.LocalTarget(path=self.assets('crossref.json.gz'))


class CrossrefItems(CrossrefTask):
    """
    One item per line.
    """

    def requires(self):
	return CrossrefSnippet()

    def run(self):
	output = shellout("gunzip -c {input} | jq -cr '.message.items[]?' > {output}", input=self.input().path)
	luigi.LocalTarget(output).move(self.output().path)

    def output(self):
	return luigi.LocalTarget(path=self.path(ext='ldj'))


class CrossrefIntermediateSchema(CrossrefTask):
    """
    Convert crossref into an intermediate schema.

    We use a tool called span, but it is really
    only converting one JSON format into another.
    """

    def requires(self):
	return CrossrefItems()

    def run(self):
	output = shellout("span-import -i crossref {input} > {output}", input=self.input().path)
	luigi.LocalTarget(output).move(self.output().path)

    def output(self):
	return luigi.LocalTarget(path=self.path(ext='ldj'))

if __name__ == '__main__':
    luigi.run(['CrossrefIntermediateSchema', '--workers', '1', '--local-scheduler'])
