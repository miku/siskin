#!/usr/bin/env python
# coding: utf-8

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
siskin is a set of tasks for library metadata management.
"""

from __future__ import print_function

import sys

from siskin import __version__

if sys.version_info < (2, 7) or sys.version_info.major > 2:
    print("siskin runs with Python 2.7 or higher, but not with Python 3 yet")
    sys.exit(1)

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


install_requires = [
    'BeautifulSoup>=3,<4',
    'backoff>=1.3.1',
    'argparse>=1.2',
    'astroid>=1.1.1',
    'colorama>=0.3.3',
    'decorator>=3.4.0',
    'elasticsearch>=2',
    'gluish>=0.2.7',
    'gspread>=0.2.2',
    'jsonpath-rw>=1.3.0',
    'logilab-common>=0.61.0',
    'luigi>=2.2',
    'lxml>=3.4.2',
    'marcx>=0.1.18',
    'nose>=1.3.3',
    'ply>=3.4',
    'prettytable>=0.7.2',
    'protobuf>=2.6.1',
    'pygments>=2',
    'pyisbn>=1.0.0',
    'pylint>=1.2.1',
    'pymarc>=3.0.1',
    'pyparsing>=2.0.3',
    'pymysql>=0.7',
    'python-dateutil>=2.2',
    'pytz>=2014.4',
    'requests>=2.5.1',
    'simplejson>=3.6.0',
    'six>=1.9.0',
    'snakebite>=2.5.1',
    'sqlitebck>=1.2.1',
    'ujson>=1.35',
    'urllib3>=1.10',
    'wsgiref>=0.1.2',
]

setup(name='siskin',
      version=__version__,
      description='Various sources and workflows.',
      url='https://github.com/ubleipzig/siskin',
      author='The Finc Authors',
      author_email='team@finc.info',
      packages=[
          'siskin',
          'siskin.sources',
          'siskin.workflows'
      ],
      package_dir={'siskin': 'siskin'},
      package_data={
          'siskin': [
              'assets/*',
              'assets/103/*',
              'assets/30/*',
              'assets/56_57_58/*',
              'assets/87/*',
              'assets/arxiv/*',
              'assets/datacite/*',
              'assets/js/*',
              'assets/maps/*',
              'assets/oai/*',
          ]},
      scripts=[
          'bin/taskcat',
          'bin/taskconfig',
          'bin/taskdeps-dot',
          'bin/taskdir',
          'bin/taskdo',
          'bin/taskdocs',
          'bin/taskdu',
          'bin/taskhash',
          'bin/taskhead',
          'bin/taskhelp',
          'bin/taskhome',
          'bin/taskinspect',
          'bin/taskless',
          'bin/taskls',
          'bin/tasknames',
          'bin/taskopen',
          'bin/taskoutput',
          'bin/taskps',
          'bin/taskpurge',
          'bin/taskredo',
          'bin/taskrm',
          'bin/taskstatus',
          'bin/tasktree',
          'bin/taskversion',
          'bin/taskwc',
      ],
      install_requires=install_requires,
      zip_safe=False,
      classifier=[
          'Development Status :: 4 - Beta',
          'Environment :: Console'
          'License :: GPLv3',
          'Operating System :: MacOS :: MacOS X',
          'Operating System :: POSIX',
          'Programming Language :: Python',
          'Topic :: Text Processing',
      ])
