#!/usr/bin/env python
# coding: utf-8
# pylint: disable=C0111,C0301

"""
Reuse established frameworks
============================

Goals:

* show integration with existing metadata frameworks
* show external command plus required assets (which can be shipped together)

"""

import luigi
from luigi.format import Gzip

from gluish.utils import shellout


class ArxivInput(luigi.ExternalTask):

    def output(self):
        return luigi.LocalTarget(path='inputs/arxiv.xml')


class ArxivIntermediateSchema(luigi.Task):

    def requires(self):
        return ArxivInput()

    def run(self):
        """
        Use metafacture runner to normalize harvested data. Assets can live
        side by side with code.
        """
        output = shellout("""flux.sh assets/arxiv.flux in={input} MAP_DIR=assets/maps/ | gzip -c > {output}""",
                          input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path='outputs/arxiv.is.ldj.gz', format=Gzip)


if __name__ == '__main__':
    luigi.run(local_scheduler=True)
