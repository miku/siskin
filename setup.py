#!/usr/bin/env python
# coding: utf-8
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
siskin is a set of tasks for library metadata management.
~~~~~~
"""

import sys

if sys.version_info < (2, 7) or sys.version_info.major > 2:
    print("siskin runs with python 2.7 or higher, but not with python 3 yet")
    sys.exit(1)

try:
    from setuptools import setup
except:
    from distutils.core import setup

from siskin import __version__

install_requires = [
    'BeautifulSoup>=3,<4',
    'argparse>=1.2',
    'astroid>=1.1.1',
    'colorama>=0.3.3',
    'decorator>=3.4.0',
    'elasticsearch>=2',
    'gluish>=0.2',
    'gspread>=0.2.2',
    'jsonpath-rw>=1.3.0',
    'logilab-common>=0.61.0',
    'luigi>=1.1.2',
    'lxml>=3.4.2',
    'marcx>=0.1.17',
    'nose>=1.3.3',
    'ply>=3.4',
    'prettytable>=0.7.2',
    'protobuf>=2.6.1',
    'pyisbn>=1.0.0',
    'pylint>=1.2.1',
    'pymarc>=3.0.1',
    'pyparsing>=2.0.3',
    'python-dateutil>=2.2',
    'pytz>=2014.4',
    'requests>=2.5.1',
    'simplejson>=3.6.0',
    'six>=1.9.0',
    'snakebite>=2.5.1',
    'sqlitebck>=1.2.1',
    'urllib3>=1.10',
    'wsgiref>=0.1.2',
]

setup(name='siskin',
      version=__version__,
      description='Various sources and workflows.',
      url='https://github.com/miku/siskin',
      author='Martin Czygan',
      author_email='martin.czygan@gmail.com',
      packages=[
        'siskin',
        'siskin.sources',
        'siskin.workflows'
      ],
      package_dir={'siskin': 'siskin'},
      package_data={'siskin': ['assets/*']},
      scripts=[
        'bin/taskcat',
        'bin/taskcp',
        'bin/taskdeps-dot',
        'bin/taskdir',
        'bin/taskdo',
        'bin/taskdu',
        'bin/taskhead',
        'bin/taskhelp',
        'bin/taskhome',
        'bin/taskindex-delete',
        'bin/taskless',
        'bin/taskls',
        'bin/taskman',
        'bin/tasknames',
        'bin/taskopen',
        'bin/taskoutput',
        'bin/taskredo',
        'bin/taskrm',
        'bin/taskstatus',
        'bin/tasktree',
        'bin/taskversion',
        'bin/taskwc',
      ],
      install_requires=install_requires,
)
