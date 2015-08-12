# coding: utf-8
# pylint: disable=C0103,W0232
"""
Define a siskin wide task with artefacts under core.home directory.
"""

from gluish.task import BaseTask
from gluish.utils import shellout
from siskin.configuration import Config
import logging
import os
import random
import siskin

config = Config.instance()

class DefaultTask(BaseTask):
    """ A base task that sets its base directory based on config value. """
    BASE = config.get('core', 'home', Config.NO_DEFAULT)

    def assets(self, path):
        """ Return the absolute path to the asset. `path` is the relative path
        below the assets root dir. """
        return os.path.join(os.path.dirname(__file__), 'assets', path)

    @property
    def logger(self):
        # logging uses singleton internally, so no worries
        return logging.getLogger('siskin')

    @property
    def logfile(self):
        """ A logfile per task for stderr redirects. """
        name = self.taskdir().split('/')[-1]
        return os.path.join(config.get('core', 'logdir', '/var/log/siskin'), '%s.log' % name)
