# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

"""
World factbook related tasks.
"""

from gluish.intervals import yearly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
import datetime
import luigi

class FactbookTask(DefaultTask):
    """ Base task for factbook. """
    TAG = 'factbook'

    def closest(self):
        return yearly(self.date)

class FactbookXML(FactbookTask):
    """ Download factbook XML version. There may be better versions. """
    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        url = 'http://jmatchparser.sourceforge.net/factbook/data/factbook.xml.gz'
        output = shellout("curl --connect-timeout 10 {url} | gunzip -c > {output}", url=url)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))
