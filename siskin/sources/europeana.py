# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

"""
Europeana NT.
"""

from gluish.benchmark import timed
from gluish.format import TSV
from gluish.path import iterfiles
from gluish.utils import shellout
from siskin.task import DefaultTask
import glob
import luigi
import os
import shutil
import tempfile

class EuropeanaTask(DefaultTask):
    TAG = 'europeana'

class EuropeanaDownload(EuropeanaTask):
    """ Download Europeana dump. """
    base = luigi.Parameter(default='http://data.europeana.eu/download', significant=False)
    version = luigi.Parameter(default='2.0')
    format = luigi.Parameter(default='nt')

    @timed
    def run(self):
        target = os.path.join(self.taskdir(), self.version, self.format)
        if not os.path.exists(target):
            os.makedirs(target)
        url = os.path.join(self.base, self.version, "datasets", self.format)
        stopover = tempfile.mkdtemp(prefix='siskin-')
        shellout("""wget -q -nd -P {directory} -rc -np -A.{format}.gz '{url}'""",
                    url=url, directory=stopover, format=self.format)
        for path in glob.glob(unicode(os.path.join(stopover, '*'))):
            dst = os.path.join(target, os.path.basename(path))
            if not os.path.exists(dst):
                # this is atomic given path and target are on the same device
                shutil.move(path, target)
        with self.output().open('w') as output:
            for path in iterfiles(target, fun=lambda p: p.endswith('nt.gz')):
                output.write_tsv(self.version, self.format, path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='filelist'), format=TSV)
