# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301

# Copyright 2018 by Leipzig University Library, http://ub.uni-leipzig.de
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
Helper for sending mail.
"""

import logging
import smtplib
import socket

from siskin.configuration import Config

config = Config.instance()
logger = logging.getLogger("siskin")

DEFAULT_SUBJECT_PREFIX = "[siskin at %s]" % (socket.gethostname())

def send_mail(sender=None, tolist=None, subject=None, message=None, smtp=None):
    """
    Send out an email. Configure `smtp`, `default-sender` in `core` config
    section. A subject prefix is always prepended. The `tolist` parameter can
    be a single string or a list of strings.

    XXX: ATM no FROM: line, maybe add that.
    """
    if subject is None:
        subject = DEFAULT_SUBJECT
    if smtp is None:
        smtp = config.get("core", "smtp")
    if sender is None:
        sender = config.get("core", "default-sender")

    if not all((sender, tolist, subject, smtp, message)):
        raise ValueError("missing sender, recipients, subject, message or smtp: %s" % locals())

    if not isinstance(tolist, list):
        tolist = [tolist]

    server = smtplib.SMTP(smtp)
    msg = 'Subject: {} {}\n\n{}'.format(DEFAULT_SUBJECT_PREFIX, subject, message)
    server.sendmail(sender, tolist, msg)
    server.quit()

    logger.debug("sent mail (%d) to %s -- %s", len(message), ", ".join(tolist), subject)

