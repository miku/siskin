# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

from gluish.benchmark import timed
from gluish.common import OAIHarvestChunk
from gluish.esindex import CopyToIndex
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
import datetime
import luigi
import pymarc

class DOABTask(DefaultTask):
    TAG = '026'

    def closest(self):
        return monthly(date=self.date)

class DOABCombine(DOABTask):
    """ Combine the chunks for a date range into a single file.
    The filename will carry a name, that will only include year
    and month for begin and end. """

    begin = luigi.DateParameter(default=datetime.date(2010, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())
    prefix = luigi.Parameter(default="marcxml", significant=False)
    url = luigi.Parameter(default="http://www.doabooks.org/oai", significant=False)
    collection = luigi.Parameter(default=None, significant=False)

    def requires(self):
        return OAIHarvestChunk(begin=self.begin, end=self.date,
                               prefix=self.prefix, url=self.url,
                               collection=self.collection)

    @timed
    def run(self):
        tmp = shellout("""yaz-marcdump -l 9=97 -f UTF8 -t UTF8
                          -i marcxml -o marc {input} > {output}""",
                          input=self.input().path, ignoremap={5: 'INVESTIGATE'})

        # filter dups, bad ids and unreadable records ...
        # TODO: make this possible with gomarckit as well
        seen = set()
        with open(tmp) as handle:
            with self.output().open('w') as output:
                reader = pymarc.MARCReader(handle, to_unicode=True)
                writer = pymarc.MARCWriter(output)
                while True:
                    try:
                        record = reader.next()
                        record_id = record['001'].value()
                        if record_id in ('15270', '15298', '15318', '15335'):
                            self.logger.debug("Skipping {0}".format(record_id))
                            continue
                        if not record_id in seen:
                            writer.write(record)
                            seen.add(record_id)
                        else:
                            self.logger.debug("Skipping duplicate: {0}".format(record_id))
                    except pymarc.exceptions.RecordDirectoryInvalid as err:
                        self.logger.warn(err)
                    except StopIteration:
                        break

    def output(self):
        """ Use monthly files. """
        return luigi.LocalTarget(path=self.path(digest=True, ext='mrc'))

class DOABJson(DOABTask):
    """ Convert to JSON. """

    begin = luigi.DateParameter(default=datetime.date(2010, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return DOABCombine(begin=self.begin, date=self.date)

    def run(self):
        output = shellout("marctojson -m date={date} {input} > {output}",
                          input=self.input().path, date=self.date)
        luigi.File(output).move(self.output().path)

    def output(self):
        """ Use monthly files. """
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class DOABIndex(DOABTask, CopyToIndex):
    begin = luigi.DateParameter(default=datetime.date(2010, 1, 1))
    date = ClosestDateParameter(default=datetime.date.today())

    index = "doab"
    doc_type = "title"
    purge_existing_index = True

    mapping = {'title': {'date_detection': False,
                          '_id': {'path': 'content.001'},
                          '_all': {'enabled': True,
                                   'term_vector': 'with_positions_offsets',
                                   'store': True}}}

    def update_id(self):
        """ This id will be a unique identifier for this indexing task."""
        return self.effective_task_id()

    def requires(self):
        return DOABJson(begin=self.begin, date=self.date)
