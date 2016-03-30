# coding: utf-8
# pylint: disable=C0103

#  Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                    The Finc Authors, http://finc.info
#                    Martin Czygan, <martin.czygan@uni-leipzig.de>
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
Define version, disable some warnings and ensure temporary and log directories
are there and writeable.
"""

from __future__ import print_function
from siskin.configuration import Config
import os
import sys
import tempfile

# TODO(miku): 2.7 pytz seems to import module argparse twice?
import warnings
warnings.filterwarnings("ignore")

# https://urllib3.readthedocs.org/en/latest/security.html#insecurerequestwarning
# wrap into try-except, since at install-time urllib3 might not be installed yet
try:
    import urllib3
    urllib3.disable_warnings()
except ImportError:
    pass

__version__ = '0.0.150'

config = Config.instance()

tempfile.tempdir = config.get('core', 'tempdir', tempfile.gettempdir())
logdir = config.get('core', 'logdir', '/var/log/siskin')

for name, directory in (('core.tempdir', tempfile.tempdir), ('core.logdir', logdir)):
    if not os.path.exists(directory):
        try:
            os.makedirs(directory, 1777)
        except OSError as err:
            msg = '{0} does not exist and we cannot create it: {1}'.format(name, directory)
            print(msg, file=sys.stderr)
            sys.exit(1)
