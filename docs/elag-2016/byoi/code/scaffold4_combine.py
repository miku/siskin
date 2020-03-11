#!/usr/bin/env python

"""
Part 4, crossref + doaj
=======================

Combined crossref + doaj.

----

To run:

    (vm) $ python scaffold4_combine.py

To clean:

    (vm) $ make clean-4

"""

import luigi
from gluish.task import BaseTask
from gluish.utils import shellout
from part2_crossref import CrossrefIntermediateSchema
from part3_doaj import DOAJIntermediateSchema


class Task(BaseTask):
    BASE = 'output'
    TAG = '4'

class CombinedIntermediateSchema(Task):
    """
    Combine both things.
    """
    def requires(self):
        """
        TODO: require intermediate schema for crossref and doaj.
        """
        

    def run(self):
        """
        TODO: Concatenate all input files.
        """
        _, tmpfile = tempfile.mkstemp(prefix='byoi-')
        # TODO: loop over inputs and run `cat`
        luigi.File(tmpfile).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))

if __name__ == '__main__':
    luigi.run(['CombinedIntermediateSchema', '--workers', '1', '--local-scheduler'])
