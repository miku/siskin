#!/usr/bin/env python
# coding: utf-8

"""
siskin is a set of tasks for library metadata management.
~~~~~~
"""

import sys

try:
    from setuptools import setup
except:
    from distutils.core import setup

install_requires = [
    'BeautifulSoup==3.2.1',
    'MySQL-python==1.2.5',
    'argparse==1.2.1',
    'astroid==1.1.1',
    'colorama==0.3.3',
    'decorator==3.4.0',
    'elasticsearch==1.3.0',
    'gluish==0.1.70',
    'gspread==0.2.1',
    'jsonpath-rw==1.3.0',
    'logilab-common==0.61.0',
    'luigi==1.0.20',
    'marcx==0.1.17',
    'nose==1.3.3',
    'ply==3.4',
    'prettytable==0.7.2',
    'pyisbn==1.0.0',
    'pylint==1.2.1',
    'pymarc==3.0.1',
    'pyparsing==2.0.3',
    'python-dateutil==2.2',
    'pytz==2014.4',
    'requests==2.5.1',
    'simplejson==3.6.0',
    'six==1.9.0',
    'snakebite==2.5.1',
    'sqlitebck==1.2.1',
    'urllib3==1.10',
    'wsgiref==0.1.2',
]

if sys.version_info < (2, 7):
    install_requires.append('importlib==1.0.2')

setup(name='siskin',
      version='0.0.94',
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
      # adjust the globs here (http://stackoverflow.com/a/3712682/89391)
      package_data={'siskin': ['assets/*.xsl',
                               'assets/*.php',
                               'assets/*.conf',
                               'assets/*.txt',
                               'assets/ambience/*']},
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
      setup_requires=['numpy==1.8.1'],
      install_requires=install_requires,
)
