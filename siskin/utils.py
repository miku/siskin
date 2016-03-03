# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301,C0103,W0614,W0401
#
#  Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                 by The Finc Authors, http://finc.info
#                 by Martin Czygan, <martin.czygan@uni-leipzig.de>
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
#

"""
Various utilities.
"""

from __future__ import print_function
from dateutil import relativedelta
from luigi.task import Register, flatten
from siskin import __version__
import collections
import cStringIO as StringIO
import errno
import functools
import hashlib
import itertools
import json
import luigi
import os
import pprint
import random
import requests
import string
import sys
import tempfile

MAN_HEADER = r"""

  \V/    \V/    \V/    \V/    \V/    \V/    \V/    \V/    \V/    \V/    \V/
---o------o------o------o------o------o------o------o------o------o------o-----

+ Siskin
"""

class SetEncoder(json.JSONEncoder):
    """ Helper to encode python sets into JSON lists. """
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)

def wc(path):
    """
    Like `wc -l`.
    """
    with open(path) as handle:
        num_lines = sum(1 for line in handle)
    return num_lines

def date_range(start_date, end_date, increment, period):
    """
    Generate `date` objects between `start_date` and `end_date` in `increment`
    `period` intervals.
    """
    result = []
    nxt = start_date
    delta = relativedelta.relativedelta(**{period:increment})
    while nxt <= end_date:
        result.append(nxt)
        nxt += delta
    return result

def copyregions(src, dst, seekmap):
    """
    Copy regions of a source file to a target. The regions are given in the
    iterable `seekmap`, which must contain (offset, length) tuples.

    `src` (r) and `dst` (w) must be open files handles.

    More: http://stackoverflow.com/q/18551592/89391
    """
    for offset, length in sorted(seekmap):
        src.seek(offset)
        dst.write(src.read(length))

class memoize(object):
    '''Decorator. Caches a function's return value each time it is called.
    If called later with the same arguments, the cached value is returned
    (not reevaluated).
    '''
    def __init__(self, func):
        self.func = func
        self.cache = {}
    def __call__(self, *args):
        if not isinstance(args, collections.Hashable):
            # uncacheable. a list, for instance.
            # better to not cache than blow up.
            return self.func(*args)
        if args in self.cache:
            return self.cache[args]
        else:
            value = self.func(*args)
            self.cache[args] = value
            return value
    def __repr__(self):
        '''Return the function's docstring.'''
        return self.func.__doc__
    def __get__(self, obj, objtype):
        '''Support instance methods.'''
        return functools.partial(self.__call__, obj)

def iterfiles(directory='.', fun=None):
    """
    Yield paths below a directory, optionally filter the path through a function
    given in `fun`.
    """
    if fun is None:
        fun = lambda path: True
    for root, dirs, files in os.walk(directory):
        for f in files:
            path = os.path.join(root, f)
            if fun(path):
                yield path

def generate_tasks_manual():
    """ Return a formatted listing of all tasks with their descriptions. """
    from siskin.sources import *
    from siskin.workflows import *

    output = StringIO.StringIO()
    # task_tuples = sorted(Register.get_reg().iteritems())
    task_names = Register.task_names()
    output.write(MAN_HEADER)
    output.write('  {0} tasks found\n\n'.format(len(task_names)))

    for name in task_names:
        klass = Register.get_task_cls(name)
        doc = klass.__doc__ or "@TODO: docs"
        output.write('{0} {1}\n'.format(name, doc))

        try:
            deps = flatten(klass().requires())
        except Exception:
            # TODO: tasks that have required arguments will fail here
            formatted = "\tUnavailable since task has required parameters."
        else:
            formatted = '\t{0}'.format(pprint.pformat(deps).replace('\n', '\n\t'))
        output.write('\n\tDependencies ({0}):\n\n{1}\n\n'.format(len(deps), formatted))

    return output.getvalue()

def random_string(length=16):
    """
    Return a random string (upper and lowercase letters) of length `length`,
    defaults to 16.
    """
    return ''.join(random.choice(string.letters) for _ in range(length))

def pairwise(obj):
    """ Iterator over a iterable in steps of two. """
    iterable = iter(obj)
    return itertools.izip(iterable, iterable)


def nwise(iterable, n=2):
    """
    Generalized :func:`pairwise`.
    Split an iterable after every `n` items.
    """
    i = iter(iterable)
    piece = tuple(itertools.islice(i, n))
    while piece:
        yield piece
        piece = tuple(itertools.islice(i, n))

def get_task_import_cache():
    """
    Load `taskname: modulename` mappings from dictionary. Return a tuple containing
    the dictionary and the path to the cache file.
    """
    task_import_cache = None
    path = os.path.join(tempfile.gettempdir(), 'siskin_task_import_cache_%s' % __version__)
    if not os.path.exists(path):
        from siskin.sources import *
        from siskin.workflows import *
        with open(path, 'w') as output:
            task_import_cache = dict([(name, Register.get_task_cls(name).__module__) for name in Register.task_names() if name[0].isupper()])
            json.dump(task_import_cache, output)

    if task_import_cache is None:
        with open(path) as handle:
            try:
                task_import_cache = json.load(handle)
            except Exception as err:
                print("failed load task import cache, try removing %s and then try again" % path, file=sys.stderr)
                sys.exit(1)

    return task_import_cache, path

class URLCache(object):
    """ A simple URL *content* cache. Stores everything on the filesystem.
    Content is first written to a temporary file and then renamed.
    With concurrent requests for the same URL, the last one wins.
    Raises exception on any statuscode >= 400. """

    def __init__(self, directory=None):
        self.directory = directory or tempfile.gettempdir()
        self.sess = requests.session()

    def get_cache_file(self, url):
        digest = hashlib.sha1(url).hexdigest()
        d0, d1, d2 = digest[:2], digest[2:4], digest[4:6]
        path = os.path.join(self.directory, d0, d1, d2)
        if not os.path.exists(path):
            try:
                os.makedirs(path)
            except OSError as e:
                if e.errno == errno.EEXIST:
                    pass
                else:
                    raise
        return os.path.join(path, digest)

    def is_cached(self, url):
        return os.path.exists(self.get_cache_file(url))

    def get(self, url):
        if not self.is_cached(url):
            r = self.sess.get(url)
            if r.status_code >= 400:
                raise RuntimeError('%s on %s' % (r.status_code, url))
            with tempfile.NamedTemporaryFile(delete=False) as output:
                output.write(r.text)
            os.rename(output.name, self.get_cache_file(url))

        with open(self.get_cache_file(url)) as handle:
            return handle.read()

class ElasticsearchMixin(luigi.Task):
    """ A small mixin for tasks that require an ES connection. """
    es_host = luigi.Parameter(default='localhost', significant=False,
                              description='elasticsearch host')
    es_port = luigi.IntParameter(default=9200, significant=False,
                                 description='elasticsearch port')
