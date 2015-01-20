# coding: utf-8

from gluish.benchmark import timed
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
import datetime
import luigi
import urllib

class ZDBTask(DefaultTask):
    TAG = 'zdb'

    def closest(self):
        return monthly(date=self.date)

class ZDBDump(ZDBTask):
    """ Download the ZDB snapshot. """

    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='ttl', description='alternatively: rdf')

    @timed
    def run(self):
        host = "datendienst.dnb.de"
        path = "/cgi-bin/mabit.pl"
        params = {
            "userID": "opendata",
            "pass": "opendata",
            "cmd": "fetch",
            "mabheft": "ZDBTitel.%s.gz" % self.format,
        }

        url = 'http://{host}{path}?{params}'.format(host=host,
                                                    path=path,
                                                    params=urllib.urlencode(params))
        output = shellout("""wget --retry-connrefused "{url}" -O {output}""", url=url)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='%s.gz' % self.format))

class ZDBExtract(ZDBTask):
    """ Extract the archive. """

    date = ClosestDateParameter(default=datetime.date.today())
    format = luigi.Parameter(default='ttl', description='alternatively: rdf')

    def requires(self):
        return ZDBDump(date=self.date, format=self.format)

    @timed
    def run(self):
        output = shellout("gunzip -c {input} > {output}", input=self.input().fn)
        luigi.File(output).move(self.output().fn)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext=self.format))

class ZDBNTriples(ZDBTask):
    """ Get a Ntriples representation of ZDB. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return ZDBExtract(date=self.date)

    @timed
    def run(self):
        output = shellout("serdi -b -i turtle -o ntriples {input} > {output}",
                          input=self.input().path)
        # output = shellout("sort {input} > {output}", input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='nt'))
