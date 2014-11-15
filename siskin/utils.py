# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301,C0103,W0614,W0401

"""
Various utilities.
"""

from __future__ import print_function
from gluish import colors
from gluish.common import IndexIdList, IndexFieldList
from luigi.task import Register, flatten
import cStringIO as StringIO
from siskin import __version__
import json
import os
import pprint
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
    task_tuples = sorted(Register.get_reg().iteritems())
    output.write(MAN_HEADER)
    output.write('  {0} tasks found\n\n'.format(len(task_tuples)))

    for name, klass in task_tuples:
        doc = klass.__doc__ or colors.red("@todo")
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
            task_import_cache = dict([(name, klass.__module__) for name, klass in Register.get_reg().iteritems()])
            json.dump(task_import_cache, output)

    if task_import_cache is None:
        with open(path) as handle:
            task_import_cache = json.load(handle)

    return task_import_cache, path
