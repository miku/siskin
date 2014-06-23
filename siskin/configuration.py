#!/usr/bin/env python
# coding: utf-8
# pylint: disable=C0301,R0904,W0221

"""
Configuration handling, taken from luigi. File format is ini. File location
at the moment is fixed at /etc/siskin/siskin.ini.
"""

from ConfigParser import ConfigParser, NoOptionError, NoSectionError
import datetime


class Config(ConfigParser):
    """ Wrapper around /etc/siskin/siskin.ini
    """
    NO_DEFAULT = None
    _instance = None
    _config_paths = ['/etc/siskin/siskin.ini']

    @classmethod
    def add_config_path(cls, path):
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
        return self._instance.read(self._config_paths)

    def _get_with_default(self, method, section, option, default, expected_type=None):
        """ Gets the value of the section/option using method. Returns default if value
        is not found. Raises an exception if the default value is not None and doesn't match
        the expected_type.
        """
        try:
            return method(self, section, option)
        except (NoOptionError, NoSectionError):
            if default is Config.NO_DEFAULT:
                raise
            if expected_type is not None and default is not None and not isinstance(default, expected_type):
                raise
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
