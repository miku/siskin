# coding: utf-8

from gluish.intervals import yearly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
import datetime
import json
import luigi

class BartocTask(DefaultTask):
    """ Bartoc. """
    TAG = 'bartoc'

    def closest(self):
        return yearly(self.date)

class BartocDump(BartocTask):
    """ Dump JSON. """

    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("curl -sL http://bartoc.org/download/json > {output}")
        with open(output) as handle:
            try:
                _ = json.load(handle)
            except ValueError as err:
                raise RuntimeError('invalid download: %s' % err)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='json'))
