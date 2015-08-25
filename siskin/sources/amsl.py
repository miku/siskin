#!/usr/bin/env python
# coding: utf-8

"""
Electronic Resource Management System Based on Linked Data Technologies.

http://amsl.technology

Config:

[amsl]

endpoint = http://111.11.111.111:8890/sparql

"""

from siskin.configuration import Config
from siskin.task import DefaultTask
import datetime
import json
import luigi
import requests
import urllib

config = Config.instance()

class AMSLTask(DefaultTask):
    TAG = 'amsl'

class AMSLISILForSourceID(AMSLTask):
    """
    Query amsl to find ISILs for source ID. This will dump the raw query
    results.
    """

    date = luigi.DateParameter(default=datetime.date.today())
    source_id = luigi.Parameter(default='0')
    format = luigi.Parameter(default='json')
    url = luigi.Parameter(default=config.get('amsl', 'endpoint'),
                          description='sparql query endpoint url', significant=False)

    def run(self):
        with open(self.assets('amsl.collections.sparql')) as handle:
            template = handle.read()
        qs = template.replace("SOURCE_ID", self.source_id)

        r = requests.get("%s?%s" % (self.url, urllib.urlencode({'query': qs, 'format': self.format})))
        if not r.status_code == 200:
            raise RuntimeError("%s %s" % (r.status_code, r.text))

        with self.output().open('w') as output:
            output.write(r.text.encode('utf-8').strip())

    def output(self):
       return luigi.LocalTarget(path=self.path(ext=self.format))
