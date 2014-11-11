# coding: utf-8

from gluish.intervals import quarterly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
import datetime
import luigi
import tempfile

class GeonamesTask(DefaultTask):
	TAG = 'geonames'

	def closest(self):
		return quarterly(date=self.date)

class GeonamesDump(GeonamesTask):
	""" Download dump. zipfile assumed, with a single file called
	all-geonames-rdf.txt inside. """

	date = ClosestDateParameter(default=datetime.date.today())

	def run(self):
		output = shellout("wget --retry-connrefused -O {output} http://download.geonames.org/all-geonames-rdf.zip")
		output = shellout("unzip -c {zipfile} 'all-geonames-rdf.txt' > {output}", zipfile=output)
		luigi.File(output).move(self.output().path)

	def output(self):
		return luigi.LocalTarget(path=self.path())
