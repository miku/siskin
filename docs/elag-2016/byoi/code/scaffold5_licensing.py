#!/usr/bin/env python

"""
Part 5, licensing
=================

Apply licensing.

----

To run:

    (vm) $ python scaffold5_licensing.py

To clean:

    (vm) $ make clean-5

"""

from __future__ import print_function

import os

import luigi
from gluish.task import BaseTask
from gluish.utils import shellout
from part4_combine import CombinedIntermediateSchema


class Task(BaseTask):
    BASE = 'output'
    TAG = '5'

    def inputdir(self):
        __dir__ = os.path.dirname(os.path.realpath(__file__))
        return os.path.join(__dir__, '../input')

class HoldingFile(luigi.ExternalTask, Task):
    """ Example holdings file. """

    def output(self):
        path = os.path.join(self.inputdir(), 'licensing/date-2016-04-19-isil-DE-15.tsv')
        return luigi.LocalTarget(path=path)

class CreateConfiguration(Task):
    """
    Fill out blanks in configuration templates.
    """
    def requires(self):
        return HoldingFile()

    def run(self):
        with open(os.path.join(self.inputdir(), 'licensing/filterconfig.json.template')) as handle:
            template = handle.read()
        with self.output().open('w') as output:
            output.write(template.replace('{{ file }}', self.input().path))

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='json'))

class ApplyLicensing(Task):
    """
    Use config to run licensing.
    """
    def requires(self):
        """
        TODO: require two things: an input file and a configuration. You can return a dictionary here.
        """

    def run(self):
        output = shellout('span-tag -c {config} <(unpigz -c {input}) | pigz -c > {output}',
                          config=self.input().get('config').path,
                          input=self.input().get('input').path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))

if __name__ == '__main__':
    luigi.run(['ApplyLicensing', '--workers', '1', '--local-scheduler'])
