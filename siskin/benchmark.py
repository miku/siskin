#!/usr/bin/env python
# coding: utf-8

"""
Provides a basic benchmark decorator. Usage:

    @timed
    def run(self):
        print('Running...')

Just logs the output. Could be extended to send this information to some service
if available.
"""

from timeit import default_timer
import functools
import logging
import os

logger = logging.getLogger('gluish')

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def red(s):
    return bcolors.FAIL + s + bcolors.ENDC

def yellow(s):
    return bcolors.WARNING + s + bcolors.ENDC

def green(s):
    return bcolors.OKGREEN + s + bcolors.ENDC

class Timer(object):
    """ A timer as a context manager. """

    def __init__(self, green=10, yellow=60):
        """ Indicate runtimes with colors, green < 10s, yellow < 60s. """
        self.timer = default_timer
        # measures wall clock time, not CPU time!
        # On Unix systems, it corresponds to time.time
        # On Windows systems, it corresponds to time.clock

        self.green = green
        self.yellow = yellow

    def __enter__(self):
        self.start = self.timer() # measure start time
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.end = self.timer() # measure end time
        self.elapsed_s = self.end - self.start # elapsed time, in seconds
        self.elapsed_ms = self.elapsed_s * 1000  # elapsed time, in milliseconds


def timed(method):
    """ A @timed decorator. """
    @functools.wraps(method)
    def _timed(*args, **kwargs):
        """ Benchmark decorator. """
        with Timer() as timer:
            result = method(*args, **kwargs)
        klass = args[0].__class__.__name__
        fun = method.__name__

        msg = '[%s.%s] %0.5f' % (klass, fun, timer.elapsed_s)
        if timer.elapsed_s <= timer.green:
            logger.debug(green(msg))
        elif timer.elapsed_s <= timer.yellow:
            logger.debug(yellow(msg))
        else:
            logger.debug(red(msg))
        return result
    return _timed
