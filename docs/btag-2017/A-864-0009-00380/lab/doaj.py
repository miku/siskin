#!/usr/bin/env python
# coding: utf-8

"""
Convert DOAJ JSON to intermediate format.
"""

from __future__ import print_function

import luigi

from common import DefaultTask
from gluish.utils import shellout


class DOAJTask(DefaultTask):
    """ DOAJ related tasks. """
    TAG = 'doaj'


class DOAJInput(luigi.ExternalTask, DOAJTask):
    """
    A partial DOAJ JSON dump.
    """

    def output(self):
        return luigi.LocalTarget(path=self.assets('doaj.ldj'))


class DOAJIntermediateSchema(DOAJTask):
    """
    Convert DOAJ into an intermediate schema. Takes 10 minutes.
    """

    def requires(self):
        return DOAJInput()

    def run(self):
        output = shellout("span-import -i doaj {input} > {output}", input=self.input().path)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

if __name__ == '__main__':
    luigi.run(['DOAJIntermediateSchema', '--workers', '1', '--local-scheduler'])
