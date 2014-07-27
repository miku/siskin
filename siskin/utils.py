# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301,C0103,W0614,W0401

"""
Various utilities.
"""

from __future__ import print_function
from luigi.task import Register, flatten
import pprint
import cStringIO as StringIO
from gluish import colors
from gluish.common import IndexIdList, IndexFieldList
from siskin.sources import *
from siskin.workflows import *

MAN_HEADER = r"""

  \V/    \V/    \V/    \V/    \V/    \V/    \V/    \V/    \V/    \V/    \V/
---o------o------o------o------o------o------o------o------o------o------o-----

+ Siskin
"""

def generate_tasks_manual():
    """ Return a formatted listing of all tasks with their descriptions. """
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
