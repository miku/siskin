#!/usr/bin/env python
# coding: utf-8

"""
A first dependency.
"""

import json
import luigi


class IPlannerResponse(luigi.ExternalTask):
    """
    A response from http://www.professionalabstracts.com API.
    """

    def output(self):
	return luigi.LocalTarget(path='inputs/iplanner.json')


class MyTask(luigi.Task):
    """
    A welcome message written to a file.
    """

    def requires(self):
	return IPlannerResponse()

    def run(self):
	with self.input().open() as file:
	    doc = json.load(file)

	with self.output().open('w') as output:
	    output.write("Willkommen zum Hands-On Lab '%s' am DBT!\n" % doc['sessions']['1']['title'])

    def output(self):
	return luigi.LocalTarget(path='outputs/x03.txt')

if __name__ == '__main__':
    luigi.run(local_scheduler=True)
