# coding: utf-8
# pylint: disable=C0103

from __future__ import print_function
from siskin.configuration import Config
import os
import sys
import tempfile

# temporary leave this here, since on 2.7 pytz seems to import
# module argparse a second time
import warnings
warnings.filterwarnings("ignore")

# https://urllib3.readthedocs.org/en/latest/security.html#insecurerequestwarning
import urllib3
urllib3.disable_warnings()

__version__ = '0.0.116'

config = Config.instance()

tempfile.tempdir = config.get('core', 'tempdir', tempfile.gettempdir())

DEFAULT_LOG_DIR = '/var/log/siskin'
logdir = config.get('core', 'logdir', DEFAULT_LOG_DIR)

if not os.path.exists(tempfile.tempdir):
    try:
        os.makedirs(tempfile.tempdir, 1777)
    except OSError as err:
        print('temp dir does not exists and we cannot create it: {0}'.format(tempfile.tempdir))
        sys.exit(1)

if not os.path.exists(logdir):
    try:
        os.makedirs(logdir, 1777)
    except OSError as err:
        print('log dir does not exists and we cannot create it: {0}'.format(logdir))
        sys.exit(1)
