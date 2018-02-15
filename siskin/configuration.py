# coding: utf-8
# pylint: disable=C0301,R0904,W0221

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
Configuration handling, taken from luigi - ini format.
"""

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import SafeConfigParser as ConfigParser

import datetime
import logging
import os
import sys

logger = logging.getLogger('siskin')


class Config(ConfigParser):
    """
    Access to ini file.
    """
    NO_DEFAULT = None
    _instance = None

    # most specific path last
    _config_paths = [
        '/etc/siskin/siskin.ini',
        os.path.join(os.path.expanduser('~'), '.config/siskin/siskin.ini'),
    ]

    @classmethod
    def add_config_path(cls, path):
        """ Append config path. """
        cls._config_paths.append(path)
        cls._instance.reload()

    @classmethod
    def instance(cls, *args, **kwargs):
        """ Singleton getter """
        if cls._instance is None:
            cls._instance = cls(*args, **kwargs)
            _ = cls._instance.reload()

        return cls._instance

    def reload(self):
        """ Reload configuration. """
        return self._instance.read(self._config_paths)
