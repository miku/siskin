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

[core]

smtp = server.example.com
default-sender = my@mail.com
default-replyto = my@mail.com
error-email = a@c.com, d@e.com
home = /path/to/dir

[amsl]

write-url = https://live.abc.xyz/w/i/write

"""

import datetime
import logging
import os
import re
import socket
import tempfile
import traceback

import luigi

from siskin import __version__
from gluish.task import BaseTask
from gluish.utils import shellout
from siskin.configuration import Config
from siskin.mail import send_mail

config = Config.instance()


class DefaultTask(BaseTask):
    """
    Base task for all siskin tasks.

    It sets the base directory, where all task artifacts will be stored. It
    also provides shortcuts to config, assets and logging objects. A command
    line parameter named --stamp is used to optionally update timestamps in
    AMSL electronic resource management system.
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

    def on_failure(self, exception):
        """
        If a task fails, try to send an email.
        """
        try:
            tolist = self.config.get("core", "error-email").split(",")
            subject = "%s %s" % (self, datetime.datetime.today().strftime("%Y-%m-%d %H:%M"))

            # XXX: Move into a template.
            message = """
            This is siskin {version} on {host}.

            An error occured in Task {name}, this is the error message:

            {exc}

            Stacktrace:

            {tb}
            """.format(
                version=__version__,
                name=self,
                exc=exception,
                tb=traceback.format_exc(),
                host=socket.gethostname(),
            )
            message = message.encode("utf-8")
            send_mail(tolist=tolist, subject=subject, message=message)
            self.logger.debug("sent error emails to %s", ", ".join(tolist))
        except TypeError as err:
            self.logger.debug("error-email may not be configured, not sending mail: %s", err)
        except Exception as err:
            self.logger.debug("failed to send error email: %s", err)

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

        Note that if a subclass overwrites `on_success` this method is not
        called, so you have to call it manually.
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
