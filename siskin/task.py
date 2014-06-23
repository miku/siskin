# coding: utf-8
# pylint: disable=C0103,W0232
"""
Define a TSK wide task with artefacts under core.home directory.
"""

from gluish.task import BaseTask
from siskin.configuration import Config

config = Config.instance()

class DefaultTask(BaseTask):
    """ A base task that sets its base directory based on config value. """
    BASE = config.get('core', 'home', Config.NO_DEFAULT)
