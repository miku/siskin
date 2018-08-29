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
siskin is a set of tasks for library metadata management. For more information
on the project, please refer to https://finc.info.
"""

from __future__ import print_function

import sys

from siskin import __version__

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


install_requires = [
    'astroid>=1.1.1',
    'backoff>=1.3.1',
    'beautifulsoup4',
    'colorama>=0.3.3',
    'configparser>=3.5.0',
    'decorator>=3.4.0',
    'elasticsearch>=2',
    'feedparser==5.2.1',
    'future>=0.16',
    'gluish>=0.2.10',
    'gspread>=0.2.2',
    'internetarchive>=1.7.6',
    'jsonpath-rw>=1.3.0',
    'logilab-common>=0.61.0',
    'luigi>=2.2',
    'lxml>=3.4.2',
    'marcx>=0.2.9',
    'nose>=1.3.3',
    'pandas>=0.20.0',
    'parso>=0.2.0',
    'ply>=3.4',
    'prettytable>=0.7.2',
    'protobuf>=2.6.1',
    'pygments>=2',
    'pyisbn>=1.0.0',
    'pylint>=1.2.1',
    'pymarc>=3.0.1',
    'pymysql>=0.7',
    'pyparsing>=2.0.3',
    'python-dateutil>=2.5',
    'pytz>=2014.4',
    'rdflib>=4',
    'requests>=2.5.1',
    'simplejson>=3.6.0',
    'six>=1.9.0',
    'snakebite>=2.5.1',
    'tornado<5,>=4.0',
    'tqdm>=4',
    'ujson>=1.35',
    'urllib3>=1.10',
    'xlrd>=1.0.0',
    'XlsxWriter>=1.0.2',
    'xmltodict==0.11.0',
]

if sys.version_info.major < 3:
    install_requires += ['argparse>=1.2', 'wsgiref>=0.1.2']


print("""
Installation note:

    If you see an error like

    > AttributeError: 'ChangelogAwareDistribution' object has no attribute '_egg_fetcher'

    or similar from python-daemon, it's unfortunate but fixable: install
    python-daemon manually, then run setup.py again:

    $ pip install python-daemon
    $ python setup.py develop

--------
""")

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
              'assets/arxiv/*',
              'assets/datacite/*',
              'assets/js/*',
              'assets/mail/*',
              'assets/maps/*',
              'assets/oai/*',
              'assets/1/*',
              'assets/10/*',
              'assets/14/*',
              'assets/15/*',
              'assets/30/*',
              'assets/34/*',
              'assets/39/*',
              'assets/52/*',
              'assets/56_57_58/*',
              'assets/73/*',
              'assets/78/*',
              'assets/80/*',
              'assets/87/*',
              'assets/88/*',
              'assets/99/*',
              'assets/100/*',
              'assets/101/*',
              'assets/103/*',
              'assets/107/*',
              'assets/117/*',
              'assets/124/*',
              'assets/127/*',
              'assets/129/*',
              'assets/130/*',
              'assets/131/*',
              'assets/143/*',
              'assets/148/*',
              'assets/150/*',
              'assets/151/*',
              'assets/153/*',
              'assets/156/*',
              'assets/161/*',
              'assets/169/*',
          ]},
      scripts=[
          'bin/taskcat',
          'bin/taskchecksetup',
          'bin/taskcleanup',
          'bin/taskconfig',
          'bin/taskdeps',
          'bin/taskdeps-dot',
          'bin/taskdir',
          'bin/taskdo',
          'bin/taskdocs',
          'bin/taskdu',
          'bin/taskhash',
          'bin/taskhead',
          'bin/taskhelp',
          'bin/taskhome',
          'bin/taskimportcache',
          'bin/taskinspect',
          'bin/taskless',
          'bin/taskls',
          'bin/tasknames',
          'bin/taskopen',
          'bin/taskoutput',
          'bin/taskps',
          'bin/taskredo',
          'bin/taskrm',
          'bin/taskstatus',
          'bin/tasktags',
          'bin/tasktree',
          'bin/taskversion',
          'bin/taskwc',
          'bin/xmltools.py',
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
