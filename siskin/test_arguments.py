# coding: utf-8 # pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301,C0103,W0614,W0401,E0202

# Copyright 2020 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
#                   Robert Schenk, <robert.schenk@uni-leipzig.de>
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

import argparse
import datetime
import os
import shutil
import sys
import tempfile
from pathlib import Path

import configparser

from siskin.arguments import FincArgumentParser


def test_plain_parser():
    parser = FincArgumentParser()

    assert parser is not None
    assert parser.config is not None
    assert isinstance(parser.parser, argparse.ArgumentParser)


def test_no_args():
    sys.argv = ["<dummy>"]

    parser = FincArgumentParser()
    args = parser.parse_args()

    assert args is not None
    assert args.inputfile == None
    assert args.outputformat == "mrc"
    assert args.interval == "monthly"
    assert args.output_hist_size == 20
    assert args.input_hist_size == 20
    assert args.root == None
    assert args.overwrite is False
    assert args.filemap is None


def test_with_root():
    sys.argv = ["<testing>", "--root", "/tmp/siskin-test-arguments"]

    parser = FincArgumentParser()
    args = parser.parse_args()

    assert args is not None
    assert args.root == "/tmp/siskin-test-arguments"


def test_outputfilename():
    with tempfile.TemporaryDirectory(prefix='siskin-test-arguments-') as dirname:
        sys.argv = ["<dummy>", "--root", dirname]

        parser = FincArgumentParser()
        parser.parse_args()

        sid, date = '234', datetime.date.today().strftime('%Y%m01')
        filename = '{}-output-{}.fincmarc.mrc'.format(sid, date)
        assert parser.outputfilename(sid) == os.path.join(dirname, sid, filename)


def test_inputfilename():
    with tempfile.TemporaryDirectory(prefix='siskin-test-arguments-') as dirname:
        sys.argv = ["<dummy>", "--root", dirname]

        parser = FincArgumentParser()
        parser.parse_args()

        sid, date = '234', datetime.date.today().strftime('%Y%m01')
        filename = '{}-input-{}.xml'.format(sid, date)  # extension dependent on default
        assert parser.inputfilename(sid) == os.path.join(dirname, sid, filename)


def test_with_config_root():
    # Explicitly set sys.argv, since this may contain values from other test functions.
    sys.argv = ["<dummy>"]

    with tempfile.TemporaryDirectory(prefix='siskin-test-arguments-') as dirname:

        config = configparser.ConfigParser()
        config.add_section('core')
        config.set('core', 'home', dirname)

        parser = FincArgumentParser(config=config)

        assert parser.config is not None
        assert parser.sid_path("123") == os.path.join(dirname, "123")


def test_remove_old_inputfiles():

    with tempfile.TemporaryDirectory(prefix='siskin-test-arguments-') as dirname:
        sys.argv = ["<dummy>", "--interval", "daily", "--root", dirname]
        parser = FincArgumentParser()
        sid = "12345"
        if not os.path.exists(parser.sid_path(sid)):
            os.makedirs(parser.sid_path(sid))

        # Create dummy files.
        for i in range(30):
            date = (datetime.date.today() - datetime.timedelta(days=i)).strftime("%Y%m%d")
            filename = '{}-input-{}.mrc'.format(sid, date)
            of = Path(os.path.join(parser.sid_path(sid), filename))
            of.touch()

        assert len(os.listdir(parser.sid_path(sid))) == 30
        parser.remove_old_inputfiles(sid)
        assert len(os.listdir(parser.sid_path(sid))) == 20


def test_remove_old_outputfiles():

    with tempfile.TemporaryDirectory(prefix='siskin-test-arguments-') as dirname:
        sys.argv = ["<dummy>", "--interval", "daily", "--root", dirname]
        parser = FincArgumentParser()
        sid = "12345"
        if not os.path.exists(parser.sid_path(sid)):
            os.makedirs(parser.sid_path(sid))

        # Create dummy files. We are testing both the removal and the default value.
        for i in range(30):
            date = (datetime.date.today() - datetime.timedelta(days=i)).strftime("%Y%m%d")
            filename = '{}-output-{}.mrc'.format(sid, date)
            of = Path(os.path.join(parser.sid_path(sid), filename))
            of.touch()

        assert len(os.listdir(parser.sid_path(sid))) == 30
        parser.remove_old_outputfiles(sid)
        assert len(os.listdir(parser.sid_path(sid))) == 20
