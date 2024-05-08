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

    (vm) $ python scaffold1_require.py

Run, suppress noise:

    (vm) $ python scaffold1_require.py 2> /dev/null

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
        """
        TODO: Require the right input.
        """

    def run(self):
        """
        TODO: Open the input, read the JSON and output:

        <message> from <location>
        """
        with self.input().open() as handle:
            doc = json.load(handle)
        print("%s from %s" % (doc["message"], doc["location"]))

    def complete(self):
        return False


if __name__ == "__main__":
    luigi.run(["Greeting", "--workers", "1", "--local-scheduler"])

    # Alternatively, replace
    #
    #     luigi.run(['Greeting', '--workers', '1', '--local-scheduler'])
    #
    # with just
    #
    #     luigi.run()
    #
    # and pass task on the command line:
    #
    #     $ python scaffold1_require.py Greet --workers 1 --local-scheduler
