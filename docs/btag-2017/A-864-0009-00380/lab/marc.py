#!/usr/bin/env python
# coding: utf-8

"""
Work with a list of marc files. Maybe use plain pymarc.
"""

from __future__ import print_function

import datetime

import luigi

from common import DefaultTask
from gluish.utils import shellout


class MarcTask(DefaultTask):
    TAG = 'marc'


class MarcDownload(MarcTask):
    """ Download file. """

    date = luigi.DateParameter(default=datetime.date.today())

    def run(self):
	output = shellout("""wget -O {output} https://ebooks.adelaide.edu.au/meta/pg/marc.zip""")
	output = shellout("""unzip -p {input} > {output}""", input=output)
	luigi.LocalTarget(output).move(self.output().path)

    def output(self):
	return luigi.LocalTarget(path=self.path(ext='mrc'))


class MarcIntermediateSchema(MarcTask):
    """ TODO: Convert to intermediate schema. """

if __name__ == '__main__':
    luigi.run(['MarcDownload', '--workers', '1', '--local-scheduler'])
