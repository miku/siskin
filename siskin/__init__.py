# coding: utf-8
# pylint: disable=C0103

from __future__ import print_function
from siskin.configuration import Config
import os
import tempfile

config = Config.instance()
tempfile.tempdir = config.get('core', 'tempdir', tempfile.gettempdir())

if not os.path.exists(tempfile.tempdir):
    try:
        os.makedirs(tempfile.tempdir, 1777)
    except OSError as err:
        print('temp dir does not exists and we cannot create it: {}'.format(tempfile.tempdir))
