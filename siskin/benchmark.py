# coding: utf-8
# pylint: disable=C301,C0103,W0201

# Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
#                   Martin Czygan, <martin.czygan@uni-leipzig.de>
#
# This file is part of some open source application.
#
# Some open source application is free software: you can redistribute
# it and/or modify it under the terms of the GNU General Public
# License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.
#
# Some open source application is distributed in the hope that it will
# be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
#
# @license GPL-3.0+ <http://spdx.org/licenses/GPL-3.0+>

"""
Provides a basic benchmark decorator. Usage:

    @timed
    def run(self):
        print('Running...')

Just logs the output. Could be extended to send this information to some service
if available.
"""

import functools
import logging
from builtins import object
from timeit import default_timer

logger = logging.getLogger('gluish')


class bcolors(object):
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def red(s):
    """ Color string red. """
    return bcolors.FAIL + s + bcolors.ENDC


def yellow(s):
    """ Color string yellow. """
    return bcolors.WARNING + s + bcolors.ENDC


def green(s):
    """ Color string green. """
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
        self.start = self.timer()  # measure start time
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.end = self.timer()  # measure end time
        self.elapsed_s = self.end - self.start  # elapsed time, in seconds
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
