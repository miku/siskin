#!/usr/bin/env python
# coding: utf-8

"""
Electronic Resource Management System Based on Linked Data Technologies.

http://amsl.technology

Managing electronic resources has become a distinctive and important task for
libraries in recent years. The diversity of resources, changing licensing
policies and new business models of the publishers, consortial acquisition and
modern web scale discovery technologies have turned the market place of
scientific information into a complex and multidimensional construct. A state-
of-the-art management of electronic resources is dependent on flexible data
models and the capability to integrate most heterogeneous data sources.
"""

from siskin.task import DefaultTask
import datetime
import luigi

class AMSLTask(DefaultTask):
    TAG = 'amsl'

class AMSLSparql(AMSLTask):
    """ Query amsl with SPARQL. """

    date = luigi.DateParameter(default=datetime.date.today())
    query = luigi.Parameter(description='SPARQL query')
    format = luigi.Parameter(default='json')
    url = luigi.Parameter(description='sparql query endpoint url', significant=False)

    def run(self):
       self.logger.debug("Running query on %s ..." % self.url)

    def output(self):
       return luigi.LocalTarget(path=self.path())
