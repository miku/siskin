# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301,C0103,W0614,W0401

"""
Various utilities.
"""

from __future__ import print_function
from gluish import colors
from luigi.task import Register, flatten
from siskin import __version__

import cStringIO as StringIO
import errno
import hashlib
import json
import luigi
import os
import pprint
import requests
import sys
import tempfile

MAN_HEADER = r"""

  \V/    \V/    \V/    \V/    \V/    \V/    \V/    \V/    \V/    \V/    \V/
---o------o------o------o------o------o------o------o------o------o------o-----

+ Siskin
"""

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
        doc = klass.__doc__ or colors.red("@todo: docs")
        output.write('{0} {1}\n'.format(colors.green(name), doc))

        try:
            deps = flatten(klass().requires())
        except Exception:
            # TODO: tasks that have required arguments will fail here
            formatted = colors.yellow("\tUnavailable since task has required parameters.")
        else:
            formatted = '\t{0}'.format(pprint.pformat(deps).replace('\n', '\n\t'))
        output.write(colors.magenta('\n\tDependencies ({0}):\n\n{1}\n\n'.format(len(deps), formatted)))

    return output.getvalue()

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
