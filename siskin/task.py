# coding: utf-8
# pylint: disable=C0103,W0232,C0301,W0703

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
Define a siskin wide task with artifacts under core.home directory.

[amsl]

write-url = https://live.abc.xyz/w/i/write

"""

import logging
import os
import re
import tempfile

import luigi

from gluish.task import BaseTask
from gluish.utils import shellout
from siskin.configuration import Config

config = Config.instance()


class DefaultTask(BaseTask):
    """
    Base task for all siskin tasks.

    It sets the base directory, where all task artifacts will be stored. It
    also provides shortcuts to config, assets and logging objects. A command
    line parameter named stamp is used to update timestamps in AMSL electronic
    resource management system.
    """
    BASE = config.get('core', 'home', fallback=os.path.join(tempfile.gettempdir(), 'siskin-data'))

    stamp = luigi.BoolParameter(default=False,
                                description="send an updated stamp to AMSL",
                                significant=False)

    def assets(self, path):
        """
        Return the absolute path to the asset. `path` is the relative path
        below the assets root dir.
        """
        return os.path.join(os.path.dirname(__file__), 'assets', path)

    @property
    def config(self):
        """
        Return the config instance.
        """
        return config

    @property
    def logger(self):
        """
        Return the logger. Module logging uses singleton internally, so no worries.
        """
        return logging.getLogger('siskin')

    def on_success(self):
        """
        Try to send a datestamp to AMSL, but only if a couple of prerequisites are met:

        All subclasses inherit a --stamp boolean flag, which must be set. If
        the TAG of the source is not numeric, we won't do anything. If
        "amsl.write-url" configuration is not set, we will log the error, but
        do not stop processing. Finally, even if the HTTP request fails, it
        won't be fatal.

        On success, the API returns:

            < HTTP/1.1 200 OK
            < Content-Type: text/html; charset=UTF-8
            < Content-Length: 2
            ...

            OK
        """

        if not self.stamp:
            return

        if not hasattr(self, 'TAG'):
            self.logger.warn("No tag defined, skip stamping.")
            return

        if not re.match(r"^[\d]+$", self.TAG):
            self.logger.warn("Non-integer source id: %s, skip stamping.", self.TAG)
            return

        # Otherwise: Parameter 'sid' ... not a positive integer.
        sid = self.TAG.lstrip("0")

        try:
            write_url = config.get("amsl", "write-url")
            if write_url is None:
                self.logger.warn("Missing amsl.write-url configuration, skip stamping.")
                return
        except Exception as err:
            self.logger.warn("Could not stamp: %s", err)
            return

        try:
            shellout("""curl --fail -XPOST "{write_url}?do=updatetime&sid={sid}" > /dev/null """,
                     write_url=write_url, sid=sid)
        except RuntimeError as err:
            self.logger.warn(err)
            return
        else:
            self.logger.debug("Successfully stamped: %s", sid)
