#!/usr/bin/env python
# coding: utf-8

"""
A first dependency
==================

Goals:

* show luigi.ExternalTask for things, that are provided
* require a single other task
* show how to access inputs with self.input ...

"""

import json

import luigi


class IPlannerResponse(luigi.ExternalTask):
    """
    A response from http://www.professionalabstracts.com API.
    """

    def output(self):
        return luigi.LocalTarget(path="inputs/iplanner.json")


class MyTask(luigi.Task):
    """
    A welcome message written to a file.
    """

    def requires(self):
        return IPlannerResponse()

    def run(self):
        """
        Access output of required task with self.input() ...
        """
        with self.input().open() as file:
            doc = json.load(file)

        with self.output().open("w") as output:
            output.write(
                "Willkommen zum Lab '%s' am DBT 2017!\n" % doc["sessions"]["1"]["title"]
            )

    def output(self):
        return luigi.LocalTarget(path="outputs/x03.txt")


if __name__ == "__main__":
    luigi.run(local_scheduler=True)
