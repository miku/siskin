# coding: utf-8

from gluish.benchmark import timed
from gluish.common import OAIHarvestChunk, Executable, Directory
from gluish.format import TSV
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.path import iterfiles
from gluish.utils import date_range, shellout
from siskin.task import DefaultTask
import datetime
import luigi
import marcx
import os
import pymarc
import re
import tempfile
import time

class HistbestTask(DefaultTask):
    TAG = '036'

    def closest(self):
        return monthly(date=self.date)

class HistbestHarvestChunk(OAIHarvestChunk, HistbestTask):
    """ Harvest all files in chunks. """

    begin = luigi.DateParameter(default=datetime.date(2010, 1, 1))
    end = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://histbest.ub.uni-leipzig.de/oai2", significant=False)
    prefix = luigi.Parameter(default="oai_dc")
    collection = luigi.Parameter(default=None, significant=False)
    delay = luigi.Parameter(default=0, significant=False)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))

class HistbestHarvest(luigi.WrapperTask, HistbestTask):
    """ Harvest Histbest. """
    begin = luigi.DateParameter(default=datetime.date(2010, 1, 1))
    end = luigi.DateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://histbest.ub.uni-leipzig.de/oai2", significant=False)
    prefix = luigi.Parameter(default="oai_dc")
    collection = luigi.Parameter(default=None, significant=False)

    def requires(self):
        """ Only require up to the last full month. """
        begin = datetime.date(self.begin.year, self.begin.month, 1)
        end = datetime.date(self.end.year, self.end.month, 1)
        if end < self.begin:
            raise RuntimeError('Invalid range: %s - %s' % (begin, end))
        dates = date_range(begin, end, 7, 'days')
        for i, _ in enumerate(dates[:-1]):
            yield HistbestHarvestChunk(begin=dates[i], end=dates[i + 1], url=self.url,
                                       prefix=self.prefix, collection=self.collection)

    def output(self):
        return self.input()

class HistbestCombine(HistbestTask):
    """ Concatenate the chunks for a date range into a single file.
    Add an artificial 001 field to all records that is taken directly
    from 856.u, and is either a URN (7407) or a URL (2187). """
    begin = luigi.DateParameter(default=datetime.date(2010, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())
    url = luigi.Parameter(default="http://histbest.ub.uni-leipzig.de/oai2", significant=False)
    prefix = luigi.Parameter(default="oai_dc")
    collection = luigi.Parameter(default=None, significant=False)

    def requires(self):
        return {'files': HistbestHarvest(begin=self.begin, end=self.date, prefix=self.prefix,
                                         url=self.url, collection=self.collection),
                'apps': [Executable(name='xsltproc'),
                         Executable(name='yaz-marcdump'),
                         Executable(name='marcuniq')]}

    @timed
    def run(self):
        xsl = self.assets("OAIDCtoMARCXML.xsl")
        _, combined = tempfile.mkstemp(prefix='siskin-')
        for target in self.input().get('files'):
            output = shellout("xsltproc {stylesheet} {input} > {output}", stylesheet=xsl, input=target.path)
            output = shellout("yaz-marcdump -f utf-8 -t utf-8 -i marcxml -o marc {input} > {output}", input=output)
            shellout("cat {input} >> {output}", input=output, output=combined)

        with open(combined, 'r') as handle:
            with self.output().open('w') as output:
                reader = pymarc.MARCReader(handle, to_unicode=True, force_utf8=True)
                writer = pymarc.MARCWriter(output)
                for record in reader:
                    record = marcx.FatRecord.from_record(record)
                    if not record.has('001'):
                        if record.has('856.u'):
                            value = list(record.itervalues('856.u'))[0]
                            id = re.sub('(^URN:|^URL:)', '', value).decode('utf-8')
                            record.add('001', data=id)
                            writer.write(record)
                    else:
                        raise RuntimeError('Did not expect IDs in this set.')

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class HistbestGNDPages(HistbestTask):
    """
    Collect the XML pages, that link to various other entities.
    This is in part realized with a BEACON service:
    http://de.wikipedia.org/wiki/Wikipedia:BEACON
    """

    date = ClosestDateParameter(default=datetime.date.today())
    delay = luigi.IntParameter(default=5, significant=False)
    first = luigi.IntParameter(default=1, significant=False)
    last = luigi.IntParameter(default=5008, significant=False)

    def requires(self):
        return Directory(path=os.path.join(self.taskdir(), str(self.closest())))

    def run(self):
        for i in range(self.first, self.last + 1):
            padded = str(i).zfill(4)
            url = "http://histbest.ub.uni-leipzig.de/receive/UBLHistBestGND_gnd_0000%s?XSL.Style=native" % padded
            output = "%s.xml" % os.path.join(self.input().path, padded)
            if not os.path.exists(output):
                output = shellout("""wget -O "{output}" "{url}" """, url=url, output=output,
                                  ignoremap={8: '404 exit with 8'})
                time.sleep(self.delay)

        with self.output().open('w') as output:
            for path in iterfiles(self.input().path):
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)
