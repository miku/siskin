#!/usr/bin/env python

"""
Part 3, doaj
============

We repeat the same steps for DOAJ.

----

To run:

    (vm) $ python scaffold3_doaj.py

To clean output:

    (vm) $ make clean-3

"""

from __future__ import print_function

import os

import luigi
from gluish.task import BaseTask
from gluish.utils import shellout


class Task(BaseTask):
    BASE = 'output'
    TAG = '3'

    def inputdir(self):
        __dir__ = os.path.dirname(os.path.realpath(__file__))
        return os.path.join(__dir__, '../input')

class DOAJInput(luigi.ExternalTask, Task):
    """
    A single DOAJ dump.
    """
    def output(self):
        path = os.path.join(self.inputdir(), 'doaj/date-2016-02-01.ldj.gz')
        return luigi.LocalTarget(path=path)

class DOAJIntermediateSchema(Task):
    """
    Convert DOAJ into an intermediate schema. Takes 10 minutes.
    """
    def requires(self):
        """
        TODO: require the right input.
        """

    def run(self):
        """
        TODO: convert to intermediate schema.
        """
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))

if __name__ == '__main__':
    luigi.run(['DOAJIntermediateSchema', '--workers', '1', '--local-scheduler'])
