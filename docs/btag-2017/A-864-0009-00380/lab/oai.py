#!/usr/bin/env python
# coding: utf-8

"""
Harvest some OAI, live and keep a set around, if the net is down.
"""

from __future__ import print_function

import datetime
import os

import luigi

from common import DefaultTask
from gluish.utils import shellout


class HarvestTask(DefaultTask):
    TAG = 'harvest'


class HarvestInput(luigi.ExternalTask, HarvestTask):
    """
    An already harvested set.
    """

    def output(self):
	return luigi.LocalTarget(path=self.assets('doaj.ldj'))


class HarvestLive(HarvestTask):
    """
    Harvest live during the lab via metha.
    """

    endpoint = luigi.Parameter(default='http://ijoc.org/index.php/ijoc/oai', significant=False)
    date = luigi.DateParameter(default=datetime.date.today())

    def run(self):
	shellout("""metha-sync "{endpoint}" """, endpoint=self.endpoint)
	output = shellout("""metha-cat -root Records "{endpoint}" > {output}""",
			  endpoint=self.endpoint)
	luigi.LocalTarget(output).move(self.output().path)

    def output(self):
	return luigi.LocalTarget(path=self.path(ext='xml'))


class HarvestIntermediateSchema(HarvestTask):
    """
    Convert OAI output to intermediate schema.
    """
    endpoint = luigi.Parameter(default='http://ijoc.org/index.php/ijoc/oai', significant=False)
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
	return HarvestLive(endpoint=self.endpoint, date=self.date)

    def run(self):
	mapdir = 'file:///%s' % self.assets("maps/")
	output = shellout("""flux.sh {flux} in={input} MAP_DIR={mapdir} > {output}""",
			  flux=self.assets("harvest/flux.flux"), mapdir=mapdir, input=self.input().path)
	luigi.LocalTarget(output).move(self.output().path)

    def output(self):
	return luigi.LocalTarget(path=self.path(ext='ldj'))


if __name__ == '__main__':
    luigi.run(['HarvestIntermediateSchema', '--workers', '1', '--local-scheduler'])
