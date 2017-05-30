#!/usr/bin/env python
# coding: utf-8
# pylint: disable=C0111,C0301

"""
TODO: OAI/Arxiv example.
"""

import luigi
from gluish.utils import shellout


class ArxivInput(luigi.ExternalTask):

    def output(self):
	return luigi.LocalTarget(path='inputs/arxiv.xml')


class ArxivIntermediateSchema(luigi.Task):

    def requires(self):
	return ArxivInput()

    def run(self):
	output = shellout("""flux.sh assets/arxiv.flux in={input} MAP_DIR=assets/maps/ > {output}""",
			  input=self.input().path)
	luigi.LocalTarget(output).move(self.output().path)

    def output(self):
	return luigi.LocalTarget(path='outputs/arxiv.is.ldj')


if __name__ == '__main__':
    luigi.run(local_scheduler=True)
