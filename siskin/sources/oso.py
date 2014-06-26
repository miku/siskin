# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

"""
OSO.

This group of task worked once, but OSO completely redesigned their site,
so there tasks will fail now. TODO: update.
"""
from gluish.benchmark import timed
from gluish.esindex import CopyToIndex
from gluish.format import TSV
from gluish.intervals import monthly
from siskin.task import DefaultTask
from gluish.utils import shellout
import BeautifulSoup
import datetime
import hashlib
import luigi
import os
import re
import tempfile
import urlparse

class OSOTask(DefaultTask):
    """ Oxford Scholarship Online. """
    TAG = '018'

    def closest(self):
    	return monthly(self.date)

class OSOPage(OSOTask):
    """ Download page only once a day. """
    date = luigi.DateParameter(default=datetime.date.today())
    url = luigi.Parameter(
        default="http://www.oxfordscholarship.com/page/643/marc-records", significant=False)

    @timed
    def run(self):
        output = shellout("wget -q --retry-connrefused -O {output} '{url}' ", url=self.url)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='html'))

class OSOSync(OSOTask):
    """ Use "Complete Set ..." hint for download. """
    date = luigi.DateParameter(default=monthly())

    def requires(self):
        return OSOPage(date=self.date)

    @timed
    def run(self):
        with self.input().open() as handle:
            soup = BeautifulSoup.BeautifulSoup(handle.read())
        links = []
        for element in soup(text=re.compile('Complete set')):
            current = element
            for _ in range(5):
                if hasattr(current, 'name') and current.name == 'a':
                    links.append(current['href'])
                    break
                if hasattr(current, 'parent'):
                    current = current.parent
                else:
                    break

        # create a separate dir
        target = os.path.join(os.path.dirname(self.output().path),
                              str(self.date))
        if not os.path.exists(target):
            os.makedirs(target)

        downloaded = set()
        for link in links:
            url = urlparse.urljoin('http://www.oxfordscholarship.com', link)
            filename = hashlib.sha1(link).hexdigest()
            destination = os.path.join(target, filename)
            shellout("wget -q --retry-connrefused '{url}' -O {output}",
                     url=url, output=destination)
            downloaded.add(destination)

        if not downloaded:
            raise RuntimeError('no OSO files found, maybe the file pattern changed?')

        with self.output().open('w') as output:
            for path in downloaded:
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class OSOCombined(OSOTask):
    date = luigi.DateParameter(default=monthly())

    def requires(self):
        return OSOSync(date=self.date)

    @timed
    def run(self):
        _, combined = tempfile.mkstemp(prefix='tasktree-')
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                shellout("cat {input} >> {output}", input=row.path,
                         output=combined)
        output = shellout("marcuniq {input} > {output}", input=combined)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class OSOJson(OSOTask):
    date = luigi.DateParameter(default=monthly())

    def requires(self):
        return OSOCombined(date=self.date)

    @timed
    def run(self):
        output = shellout("marctojson -m date={date} {input} > {output}",
                          date=self.date, input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class OSOIndex(OSOTask, CopyToIndex):
    date = luigi.DateParameter(default=monthly())

    index = 'oso'
    doc_type = 'title'
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
        return OSOJson(date=self.date)
