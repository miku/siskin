#!/usr/bin/env python
# coding: utf-8

"""
tsk is a set of tasks for library metadata management.
"""

try:
    from setuptools import setup
except:
    from distutils.core import setup


setup(name='tsk',
      version='0.0.1',
      description='Various sources and workflows.',
      author='Martin Czygan',
      author_email='martin.czygan@gmail.com',
      packages=[
        'tsk',
        'tsk.sources',
        'tsk.workflows'
      ],
      package_dir={'tsk': 'tsk'},
      package_data={'tsk': ['assets/*']},
      scripts=[
        'bin/taskcat',
        'bin/taskdo',
        'bin/taskless',
        'bin/tasknames',
        'bin/taskoutput',
      ]
)
