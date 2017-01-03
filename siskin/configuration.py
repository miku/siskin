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
Configuration handling, taken from luigi. File format is ini. File location
at the moment is fixed at /etc/siskin/siskin.ini.
"""

import datetime
import logging
import os
import sys
from ConfigParser import ConfigParser, NoOptionError, NoSectionError

logger = logging.getLogger('siskin')


class Config(ConfigParser):
    """ Wrapper around /etc/siskin/siskin.ini
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

    def _get_with_default(self, method, section, option, default, expected_type=None):
        """ Gets the value of the section/option using method. Returns default if value
        is not found. Raises an exception if the default value is not None and doesn't match
        the expected_type.
        """
        try:
            return method(self, section, option)
        except (NoOptionError, NoSectionError) as err:
            if default is Config.NO_DEFAULT:
                logger.error('invalid or missing configuration: %s', err)
                sys.exit(1)
            if expected_type is not None and default is not None and not isinstance(default, expected_type):
                logger.error('invalid or missing configuration: %s', err)
                sys.exit(1)
            return default

    def get(self, section, option, default=NO_DEFAULT):
        return self._get_with_default(ConfigParser.get, section, option, default)

    def getboolean(self, section, option, default=NO_DEFAULT):
        return self._get_with_default(ConfigParser.getboolean, section, option, default, bool)

    def getint(self, section, option, default=NO_DEFAULT):
        return self._get_with_default(ConfigParser.getint, section, option, default, int)

    def getfloat(self, section, option, default=NO_DEFAULT):
        return self._get_with_default(ConfigParser.getfloat, section, option, default, float)

    def getdate(self, section, option, default=NO_DEFAULT):
        """ Dates must be formatted ISO8601 style: %Y-%m-%d """
        value = self.get(section, option, default=default)
        return datetime.date(*map(int, value.split('-')))
