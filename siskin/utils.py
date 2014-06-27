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
from siskin.sources.b3kat import *
from siskin.sources.bms import *
from siskin.sources.bnf import *
from siskin.sources.bsz import *
from siskin.sources.bvb import *
from siskin.sources.disson import *
from siskin.sources.doab import *
from siskin.sources.doaj import *
from siskin.sources.ebl import *
from siskin.sources.ebrary import *
from siskin.sources.elsevier import *
from siskin.sources.ema import *
from siskin.sources.freebase import *
from siskin.sources.gbv import *
from siskin.sources.gnd import *
from siskin.sources.hszigr import *
from siskin.sources.imslp import *
from siskin.sources.imdb import *
from siskin.sources.ksd import *
from siskin.sources.lfer import *
from siskin.sources.mor import *
from siskin.sources.mtc import *
from siskin.sources.naxos import *
from siskin.sources.nep import *
from siskin.sources.nl import *
from siskin.sources.oso import *
from siskin.sources.pao import *
from siskin.sources.qucosa import *
from siskin.sources.rism import *
from siskin.sources.ssoar import *
from siskin.sources.swbod import *
from siskin.sources.viaf import *
from siskin.sources.wikidata import *
from siskin.sources.wikipedia import *
from siskin.sources.yago import *

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
    output.write('+ {} tasks found\n\n'.format(len(task_tuples)))

    for name, klass in task_tuples:
        doc = klass.__doc__ or colors.red("@todo")
        output.write('{} {}\n'.format(colors.green(name), doc))

        try:
            deps = flatten(klass().requires())
        except Exception:
            # TODO: tasks that have required arguments will fail here
            formatted = colors.yellow("\tUnavailable since task has required parameters.")
        else:
            formatted = '\t{}'.format(pprint.pformat(deps).replace('\n', '\n\t'))
        output.write(colors.magenta('\n\tDependencies ({}):\n\n{}\n\n'.format(len(deps), formatted)))

    return output.getvalue()
