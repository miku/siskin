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

import sys

from siskin import __version__

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

# XXX: Break this up into production and development dependencies.
install_requires = [
    'astroid>=1.1.1,<2',
    'backoff>=1.11.1',
    'beautifulsoup4>=4', # https://pypi.org/project/bs4/
    'colorama>=0.3.3',
    'configparser>=3.5.0',
    'cython>=0.29.33',
    'decorator>=5.0.9',
    'elasticsearch>=7',
    'feedparser>=6.0.8',
    'gluish>=0.3',
    'internetarchive>=2.0.3',
    'iso-639>=0.4.5',
    'langdetect>=1.0.9',
    'logilab-common>=0.61.0',
    # 'luigi>=3', # https://github.com/spotify/luigi/issues/3202, 3.11 issue;
    # PEP0508 fix commit until release appears; seems to not work out of the
    # box on target, but this does:
    #
    # $ pip install -e git+https://github.com/luigi#egg=luigi
    #
    # 'luigi @ git+https://github.com/spotify/luigi@3de05605024bdfdb38cf011c271edb0f488a90e0#egg=luigi',
    'luigi>=3.2.1',
    'lxml>=3.4.2',
    'marcx>=0.2.12',
    'numpy',
    'pandas>=1.1',
    'pycld3>=0.22',
    'pydantic>=1.10.5',
    'pygments>=2.9.0',
    'pyisbn>=1.0.0',
    'pymarc>=3.0.1',
    'pymysql>=0.7',
    'python-dateutil>=2.8.2',
    'pytz>=2014.4',
    'rdflib>=4',
    'requests>=2.26.0',
    'responses',
    'six>=1.16.0',
    'tqdm>=4',
    'urllib3<1.27,>=1.21.1',
    'xlrd>=1.0.0',
    'xlsxwriter>=1.4.4',
    'xmltodict>=0.11.0',
]

if sys.version_info.major < 3:
    install_requires += ['argparse>=1.2', 'wsgiref>=0.1.2']


print("""
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

Installation note:

    If you see an error like

    > AttributeError: 'ChangelogAwareDistribution' object has no attribute
      '_egg_fetcher'

    or similar from python-daemon, it's unfortunate but fixable: install
      python-daemon manually, then run setup.py again:

    $ pip install python-daemon
    $ python setup.py develop

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
""")

setup(name='siskin',
      version=__version__,
      description='Luigi tasks for building an article metadata index for https://finc.info',
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
              'assets/amsl/*',
              'assets/datacite/*',
              'assets/js/*',
              'assets/mail/*',
              'assets/maps/*',
              'assets/oai/*',
              'assets/55/*',
              'assets/68/*',
              'assets/87/*',
          ]},
      scripts=[
          'bin/iconv-chunks',
          # TODO(martin): use "siskin-" prefix for all scripts
          'bin/siskin-whatislive.sh',
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
          'bin/taskgc',
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
          'bin/tasksync',
          'bin/tasktags',
          'bin/tasktree',
          'bin/taskversion',
          'bin/taskwc',
      ],
      entry_points={
        'console_scripts': [
            'siskin=siskin.main:main',
        ],
      },
      install_requires=install_requires,
      zip_safe=False,
      classifiers=[
          'Development Status :: 4 - Beta',
          'Environment :: Console',
          'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
          'Operating System :: MacOS :: MacOS X',
          'Operating System :: POSIX',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python',
          'Topic :: Text Processing',
      ])
