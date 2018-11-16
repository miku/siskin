# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301,C0103,W0614,W0401,E0202

# Copyright 2018 by Leipzig University Library, http://ub.uni-leipzig.de
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
Entry points should replace scripts in the future.
"""

from __future__ import print_function

import sys

from luigi.task import Register

from siskin.benchmark import green, yellow
from siskin.sources import *
from siskin.workflows import *
from siskin import __version__

def main():
    print("siskin %s\n\n" % __version__)
    task_names = Register.task_names()
    print('{0} tasks found\n'.format(len(task_names)))

    for name in task_names:
        if name.islower():
            continue
        klass = Register.get_task_cls(name)
        doc = klass.__doc__ or yellow("@TODO: docs")
        print('{0} {1}\n'.format(green(name), doc))

