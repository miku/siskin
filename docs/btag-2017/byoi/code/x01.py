#!/usr/bin/env python
# coding: utf-8

"""
Web Interface
=============

Goals:

* show web interface

----

First, start scheduler in the background.

    $ luigid --background

Simple command line integration.

    $ python x01.py MyTask

Screenies:

* http://i.imgur.com/OTgMTZP.png

"""

import time

import luigi


class MyTask(luigi.Task):
    """Simulate work, so we can inspect the web interface."""

    def run(self):
        """TODO: Adjust HTTP URL."""
        print("running task - visit http://127.0.0.1:8082")
        time.sleep(60)
        # TODO: Write a string of your choice to the output file of this task.

    def output(self):
        return luigi.LocalTarget(path="outputs/x01.txt")


if __name__ == "__main__":
    luigi.run()
