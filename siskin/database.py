# coding: utf-8
# pylint: disable=C0301,C0103

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
Helper for databases.
"""

import logging
import sqlite3
import six.moves.urllib.parse
from builtins import object

import pymysql
from future import standard_library
from pymysql.cursors import SSCursor

# XXX: move to six.
standard_library.install_aliases()


class sqlitedb(object):
    """
    Simple cursor context manager for sqlite3 databases. Commits everything at exit.

        with sqlitedb('/tmp/test.db') as cursor:
            query = cursor.execute('SELECT * FROM items')
            result = query.fetchall()
    """

    def __init__(self, path, timeout=5.0, detect_types=0):
        self.path = path
        self.conn = None
        self.cursor = None
        self.timeout = timeout
        self.detect_types = detect_types

    def __enter__(self):
        self.conn = sqlite3.connect(
            self.path, timeout=self.timeout, detect_types=self.detect_types)
        self.conn.text_factory = str
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, exc_class, exc, traceback):
        self.conn.commit()
        self.conn.close()


class mysqldb(object):
    """
    Context manager for MySQL database access. Example:

        with mysqldb('mysql://user:pass@host/db', stream=True) as cursor:
            query = cursor.execute('SELECT * FROM items')
            result = query.fetchall()
    """

    def __init__(self, url, stream=False, commit_on_exit=False):
        result = urllib.parse.urlparse(url, scheme='mysql')
        self.hostname = result.hostname
        self.username = result.username
        self.password = result.password
        self.database = result.path.strip('/')
        self.stream = stream
        self.commit_on_exit = commit_on_exit
        self.conn = None
        self.cursor = None

    def __enter__(self):
        if self.stream:
            self.conn = pymysql.connect(host=self.hostname, user=self.username,
                                        passwd=self.password, db=self.database,
                                        cursorclass=SSCursor)
        else:
            self.conn = pymysql.connect(host=self.hostname, user=self.username,
                                        passwd=self.password, db=self.database)

        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, exc_class, exc, traceback):
        if self.commit_on_exit:
            self.conn.commit()
        self.cursor.close()
        self.conn.close()
