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
    """ Dump JSON. http://bartoc.org/en/node/761 """

    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("""wget -O {output} 'http://bartoc.org/en/download/json?download=1&eid=3224' """)
        with open(output) as handle:
            try:
                _ = json.load(handle)
            except ValueError as err:
                raise RuntimeError('invalid download: %s' % err)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='json'))
