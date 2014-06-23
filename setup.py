#!/usr/bin/env python
# coding: utf-8

"""
siskin is a set of tasks for library metadata management.
~~~~~~
"""

try:
    from setuptools import setup
except:
    from distutils.core import setup


setup(name='siskin',
      version='0.0.2',
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
        'bin/taskdo',
        'bin/taskless',
        'bin/taskls',
        'bin/tasknames',
        'bin/taskoutput',
        'bin/taskrm',
        'bin/taskwc',
      ]
)
