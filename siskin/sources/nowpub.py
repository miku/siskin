# coding: utf-8

from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
import datetime
import luigi

class NowpubTask(DefaultTask):
    TAG = "nowpub"

    def closest(self):
        return monthly(date=self.date)

class NowpubMarc(NowpubTask):
    """ Download zipfile an combine marc records. """

    date = ClosestDateParameter(default=datetime.date.today())

    def run(self):
        output = shellout("wget -O {output} http://www.nowpublishers.com/Public-Content/marc-records/fntall.zip")
        output = shellout("unzip -p {input} *mrc > {output}", input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class NowpubJson(NowpubTask):
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return NowpubMarc(date=self.date)

    def run(self):
        output = shellout("marctojson {input} > {output}", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))
