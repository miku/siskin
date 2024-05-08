#!/usr/bin/env python

"""
Part 2, crossref
================

We start using gluish for automatic output paths and easy shell calls.

----

To run:

    (vm) $ python scaffold2_crossref.py

To clean:

    (vm) $ make clean-2

"""

from __future__ import print_function

import os
import tempfile

import luigi
from gluish.task import BaseTask
from gluish.utils import shellout


class Task(BaseTask):
    """
    All output will go to: output/2/<class>/<file>
    """

    BASE = "output"
    TAG = "2"

    def inputdir(self):
        __dir__ = os.path.dirname(os.path.realpath(__file__))
        return os.path.join(__dir__, "../input")


class CrossrefInput(Task):
    """
    List crossref files.
    """

    def run(self):
        """
        Only use the first file, so it is faster. To use more files, drop the `head -1`.
        """
        directory = os.path.join(self.inputdir(), "crossref")
        output = shellout(
            "find {directory} -name '*.ldj.gz' | head -1 > {output}",
            directory=directory,
        )
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())


class CrossrefItems(Task):
    """
    Extract the actual items from crossref API dumps. This will take about 30min.
    """

    def requires(self):
        return CrossrefInput()

    def run(self):
        """
        TODO: For each file, we want to run a jq command.
        """
        _, temp = tempfile.mkstemp(prefix="byoi-")
        with self.input().open() as handle:
            # TODO: insert code here
            pass

        luigi.File(temp).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="ldj.gz"))


class CrossrefIntermediateSchema(Task):
    """
    Convert crossref into an intermediate schema.

    We use a tool called span, but it is really
    only converting one JSON format into another.
    """

    def requires(self):
        return CrossrefItems()

    def run(self):
        """
        TODO: convert input to intermediate schema via span-import.
        """
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="ldj.gz"))


if __name__ == "__main__":
    luigi.run(["CrossrefIntermediateSchema", "--workers", "1", "--local-scheduler"])
