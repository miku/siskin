# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

"""
Bibliographie des Musikschrifttums online (BMS online).

http://www.musikbibliographie.de

Configuration keys:

[bms]

ftp-host = host.name
ftp-username = username
ftp-password = password
ftp-filepath = /path/to/file

"""

from gluish.benchmark import timed
from gluish.common import FTPFile
from gluish.esindex import CopyToIndex
from gluish.intervals import yearly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.configuration import Config
from siskin.task import DefaultTask
import datetime
import luigi

config = Config.instance()

class BMSTask(DefaultTask):
    """ BMS base task. """
    TAG = '011'

    def closest(self):
        return yearly(date=self.date)

class BMSImport(BMSTask):
    """ Fetch bmsall.xml from the FTP server. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        host = config.get('bms', 'ftp-host')
        username = config.get('bms', 'ftp-username')
        password = config.get('bms', 'ftp-password')
        filepath = config.get('bms', 'ftp-filepath')
        return FTPFile(host=host, username=username, password=password,
                       filepath=filepath)

    @timed
    def run(self):
        self.input().move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))

class BMSMarc(BMSTask):
    """ Convert the XML to MARC. """

    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return BMSImport(date=self.date)

    @timed
    def run(self):
        output = shellout("yaz-marcdump -i marcxml -o marc {input} > {output}",
                          input=self.input().path, ignoremap={5: 'INVESTIGATE'})
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class BMSJson(BMSTask):
    """ Convert the combined MARC file to a single JSON file."""
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return BMSMarc(date=self.date)

    @timed
    def run(self):
        tmp = shellout("marctojson -m date={date} {input} > {output}",
                       input=self.input().path, date=self.date)
        luigi.File(tmp).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class BMSIndex(BMSTask, CopyToIndex):

    date = ClosestDateParameter(default=datetime.date.today())

    index = 'bms'
    doc_type = 'title'
    purge_existing_index = True

    mapping = {'title': {'date_detection': False,
                          '_id': {'path': 'content.001'},
                          '_all': {'enabled': True,
                                   'term_vector': 'with_positions_offsets',
                                   'store': True}}}

    def update_id(self):
        """ This id will be a unique identifier for this indexing task. """
        return self.effective_task_id()

    def requires(self):
        return BMSJson(date=self.date)
