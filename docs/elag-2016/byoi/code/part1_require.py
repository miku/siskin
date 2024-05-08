#!/usr/bin/env python

"""
Part 1
======

A small example.

Input file:

    {
      "message": "Hello World",
      "location": "Copenhagen"
    }

----

To run:

    (vm) $ python part1_require.py HelloWorld

Run with less noise:

    (vm) $ python part1_require.py HelloWorld 2> /dev/null

"""

import json
import os

import luigi


class SomeInput(luigi.ExternalTask):
    """
    This is an external task. Something, that already exists on the file system.
    """

    def output(self):
        __dir__ = os.path.dirname(os.path.realpath(__file__))
        return luigi.LocalTarget(
            path=os.path.join(__dir__, "../input/warmup", "hello.json")
        )


class Greeting(luigi.Task):
    """This task requires `SomeInput`."""

    def requires(self):
        return SomeInput()

    def run(self):
        with self.input().open() as handle:
            doc = json.load(handle)
        print("%s from %s" % (doc["message"], doc["location"]))

    def complete(self):
        return False


if __name__ == "__main__":
    luigi.run(["Greeting", "--workers", "1", "--local-scheduler"])
    # or on the command line run:
    # $ python part1.py Greet --workers 1 --local-scheduler
