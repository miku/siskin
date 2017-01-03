# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301,C0103,W0614,W0401,E0202

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
Various utilities.
"""

from __future__ import print_function

import collections
import cStringIO as StringIO
import errno
import functools
import hashlib
import itertools
import json
import os
import pprint
import random
import string
import sys
import tempfile

import luigi
import requests
from dateutil import relativedelta

import backoff
from siskin import __version__


class SetEncoder(json.JSONEncoder):
    """ Helper to encode python sets into JSON lists. """

    def default(self, obj):
        """ Decorate call to standard implementation. """
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)


def date_range(start_date, end_date, increment, period):
    """
    Generate `date` objects between `start_date` and `end_date` in `increment`
    `period` intervals.
    """
    result = []
    nxt = start_date
    delta = relativedelta.relativedelta(**{period: increment})
    while nxt <= end_date:
        result.append(nxt)
        nxt += delta
    return result


def iterfiles(directory='.', fun=None):
    """
    Yield paths below a directory, optionally filter the path through a function
    given in `fun`.
    """
    if fun is None:
        fun = lambda path: True
    for root, _, files in os.walk(directory):
        for f in files:
            path = os.path.join(root, f)
            if fun(path):
                yield path


def random_string(length=16):
    """
    Return a random string(upper and lowercase letters) of length `length`,
    defaults to 16.
    """
    return ''.join(random.choice(string.letters) for _ in range(length))


def pairwise(obj):
    """ Iterator over a iterable in steps of two. """
    iterable = iter(obj)
    return itertools.izip(iterable, iterable)


def nwise(iterable, n=2):
    """
    Generalized: func: `pairwise`.
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
    path = os.path.join(tempfile.gettempdir(),
                        'siskin_task_import_cache_%s' % __version__)
    if not os.path.exists(path):
        from siskin.cacheutils import _write_task_import_cache
        _write_task_import_cache(path)

    with open(path) as handle:
        try:
            task_import_cache = json.load(handle)
        except Exception as err:
            print("failed load task import cache, try removing %s and then try again" %
                  path, file=sys.stderr)
            sys.exit(1)

    return task_import_cache, path


class URLCache(object):
    """
    A simple URL * content * cache. Stores everything on the filesystem. Content
    is first written to a temporary file and then renamed. With concurrent
    requests for the same URL, the last one wins. Raises exception on any HTTP
    status >= 400. Retries supported.

    >> > cache = URLCache()
    >> > cache.get_cache_file("https://www.google.com")
    /tmp / ef / 7e / fc / ef7efc9839c3ee036f023e9635bc3b056d6ee2d

    >> > cache.is_cached("https://www.google.com")
    False

    >> > page = cache.get("https://www.google.com")
    >> > page[:15]
    '<!doctype html>'

    >> > cache.is_cached("https://www.google.com")
    True
    """

    def __init__(self, directory=None, max_tries=12):
        """
        If `directory` is not explictly given, all files will be stored under
        the temporary directory. Requests can be retried, if they resulted in
        a non 200 HTTP status code. The server might send a HTTP 500 (Internal
        Server Error), even if it really is a HTTP 503 (Service Unavailable).
        We therefore treat HTTP 500 errors as something to retry on,
        `max_tries` times.
        """
        self.directory = directory or tempfile.gettempdir()
        self.sess = requests.session()
        self.max_tries = max_tries

    def get_cache_file(self, url):
        """
        Return the cache file path for a URL. This will create the parent
        directories, if necessary.
        """
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
        """
        Return URL, either from cache or the web.
        """

        @backoff.on_exception(backoff.expo, RuntimeError, max_tries=self.max_tries)
        def fetch(url):
            """
            Nested function, so we can configure number of retries.
            """
            r = self.sess.get(url, timeout=600)
            if r.status_code >= 400:
                raise RuntimeError('%s on %s' % (r.status_code, url))
            with tempfile.NamedTemporaryFile(delete=False) as output:
                output.write(r.text.encode('utf-8'))
            os.rename(output.name, self.get_cache_file(url))

        if not self.is_cached(url):
            fetch(url)

        with open(self.get_cache_file(url)) as handle:
            return handle.read()


class ElasticsearchMixin(luigi.Task):
    """ A small mixin for tasks that require an ES connection. """
    es_host = luigi.Parameter(default='localhost', significant=False,
                              description='elasticsearch host')
    es_port = luigi.IntParameter(default=9200, significant=False,
                                 description='elasticsearch port')
