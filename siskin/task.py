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

#
# ambience experiment - audio feedback to command line tasks
#

    def _ambience_ok(self):
        return os.path.join('ambience', 'ok{}.mp3'.format(random.randint(1, 12)))

    def _ambience_deny(self):
        return os.path.join('ambience', 'deny{}.mp3'.format(random.randint(1, 4)))

    def _ambience_complete(self):
        return os.path.join('ambience', 'complete.mp3')

    def _ambience_unable_to_comply(self):
        return os.path.join('ambience', 'unable_to_comply.mp3')

    def _ambience_access_denied(self):
        return os.path.join('ambience', 'access_denied.mp3')

    def _ambience_critical(self):
        return os.path.join('ambience', 'critical.mp3')

    def _ambience(self, kind='ok'):
        filenames = {'ok': self._ambience_ok(),
                     'deny': self._ambience_deny(),
                     'complete': self._ambience_complete(),
                     'unable_to_comply': self._ambience_unable_to_comply(),
                     'access_denied': self._ambience_access_denied(),
                     'critical': self._ambience_critical()}
        shellout("""mpg123 -q {path}""", path=self.assets(filenames.get(kind)))

    def on_success(self):
        if config.getboolean('core', 'ambience', False):
            self._ambience(kind='ok')

    def on_failure(self, exception):
        if config.getboolean('core', 'ambience', False):
            self._ambience(kind='deny')
