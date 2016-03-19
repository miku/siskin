# coding: utf-8
# pylint: disable=C0103,W0232

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
