#!/usr/bin/env python

"""
Part 6, export
==============

----

To run:

    (vm) $ python part6_export.py

To clean:

    (vm) $ make clean-6

"""

from __future__ import print_function


import luigi
from gluish.task import BaseTask
from gluish.utils import shellout
from part5_licensing import ApplyLicensing


class Task(BaseTask):
    BASE = 'output'
    TAG = '6'

class Export(Task):
    """
    Export. Then index into solr manually with

    $ solrbulk -w 2 -z -verbose -host localhost -port 8080 -commit 100000 -collection biblio output/6/Export/output.ldj.gz

    There are about 3253342 records. On a single core limited memory machine, this might take a while to index.
    """

    def requires(self):
        return ApplyLicensing()

    def run(self):
        output = shellout("span-export <(unpigz -c {input}) | pigz -c > {output}", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj.gz'))

if __name__ == '__main__':
    luigi.run(['Export', '--workers', '1', '--local-scheduler'])
