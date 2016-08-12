#!/usr/bin/env python
# coding: utf-8

"""
Dummy module for testing out tasks.
"""

import datetime

from siskin.task import DefaultTask


class DummyTask(DefaultTask):
    TAG = 'dummy'

class DummyHelloWorld(DummyTask):
    """
    Just say hello world.
    """
    def run(self):
        print("Hello from %s" % self.__class__.__name__)

    def complete(self):
        return False

class DummyFail(DummyTask):
    """
    A task that always fails.
    """

    def run(self):
        raise RuntimeError('This error occured at %s' % datetime.datetime.now())

    def complete(self):
        return False
