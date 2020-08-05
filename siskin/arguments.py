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
"""
Helper for argument parsing for direct processing scripts (e.g. bin/185_fincmarc.py).
"""

import argparse
import datetime
import glob
import os.path
import re
import sys

import pymarc
from gluish.intervals import monthly, weekly
from gluish.parameter import ClosestDateParameter
from siskin.configuration import Config


class FincArgumentParser():
    """
    Experimental parser grouping common operations together in a class.

    An argument parser that parses common arguments for tasks and is able to
    output various derived values, such as input and output filenames.

    Example usage:

        # somescript.py --root /tmp/abc --interval daily --format mrc
        fip = FincArgumentParser()

        # Access sid directory, input filename and output filename via fip.
        fip.sid_path("123")
        fip.inputfilename()
        fip.outputfilename()

        # If you need the args of the wrapped ArgumentParser explicitly, use:
        root = fip.args.root
    """
    def __init__(self, config=None):
        """
        Create a parser with default flags. The config is a standard library
        ConfigParser (https://docs.python.org/3/library/configparser.html#configparser.ConfigParser) object.
        """
        self.config = config
        if self.config is None:
            self.config = Config.instance()

        self.parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        self.parser.add_argument("--inputfile", dest="inputfile", help="inputfile")
        self.parser.add_argument("--inputfolder", dest="inputfolder", help="inputfolder")
        self.parser.add_argument("--inputformat", dest="inputformat", help="inputformat mrc, xml or json", default="xml")
        self.parser.add_argument("--outputformat", dest="outputformat", help="outputformat mrc or xml", default="mrc")
        self.parser.add_argument("--filemap", dest="filemap", help="path of the file containing a dictionary, list or another map")
        self.parser.add_argument("--overwrite", dest="overwrite", help="overwrite existing outputfile", nargs="?", const=True, default=False)
        self.parser.add_argument("--interval", dest="interval", help="interval for update", default="monthly")
        self.parser.add_argument("--root", dest="root", help="root path for all data")
        self.parser.add_argument("--output-hist-size", dest="output_hist_size", type=int, help="number of older outputfiles to keep", default=20)
        self.parser.add_argument("--input-hist-size", dest="input_hist_size", type=int, help="number of older inputfiles to keep", default=20)

    @property
    def args(self):
        return self.parse_args()

    def parse_args(self):
        """
        Returns the args as parsed by argparse.ArgumentParser.
        """
        return self.parser.parse_args()

    def sid_path(self, sid):
        """
        Return path (direcoty) for a given source id. This will create the
        output directory, if it does not exist. The root directory however, must
        exist before.
        """
        root = self.parse_args().root
        if not root:
            root = self.config.get("core", "home", fallback="")
        if not root:
            raise RuntimeError(sid + ": No root path for data given. Use --root or specify a default root in the siskin.ini configuration file.")
        if not os.path.exists(root):
            raise RuntimeError(sid + ": root directory does not exists: " + root)
        if not os.path.isdir(root):
            raise RuntimeError(sid + ": root needs to be a directory: " + root)

        path = os.path.join(root, sid)
        if os.path.exists(path):
            if not os.path.isdir(path):
                raise RuntimeError(sid + ": sid path exists, but is not a directory: " + path)
        else:
            os.mkdir(path)

        return path

    def remove_old_inputfiles(self, sid):
        """
        For a given sid, remove the older files.
        """
        args = self.parse_args()
        n = args.input_hist_size
        inputfilename = self.inputfilename(sid)
        inputfilenames_to_delete = re.sub("-input-.*", "-input-*", inputfilename)

        files = glob.glob(inputfilenames_to_delete)
        files = sorted(files, key=os.path.getctime)
        for filename in files[:-n]:
            os.remove(filename)

    def remove_old_outputfiles(self, sid):
        """
        For a given sid, remove the older files.
        """
        args = self.parse_args()
        n = args.output_hist_size
        outputfilename = self.outputfilename(sid)
        outputfilenames_to_delete = re.sub("-output-.*", "-output-*", outputfilename)

        files = glob.glob(outputfilenames_to_delete)
        files = sorted(files, key=os.path.getctime)
        for filename in files[:-n]:
            os.remove(filename)

    def outputfilename(self, sid):
        """
        Given a source id, return a structured output path.
        """
        args = self.parse_args()

        if args.outputformat not in ("mrc", "xml", "json"):
            raise ValueError(sid + ": Unsupported format. Choose mrc, xml or json.")
        if args.interval not in ("monthly", "weekly", "daily", "manually"):
            raise ValueError(sid + ": Unsupported interval. Choose monthly, weekly, daily or manually.")
        if args.interval == "manually" and not args.overwrite:
            raise ValueError(sid + ": Interval is manually. Use --overwrite to force a new output.")

        date = datetime.date.today()

        if args.interval == "monthly":
            date = monthly()
        if args.interval == "weekly":
            date = weekly()

        if args.outputformat == "mrc" or args.outputformat == "xml":
            filename = sid + "-output-" + date.strftime("%Y%m%d") + ".fincmarc." + args.outputformat
        elif args.outputformat == "json":
            filename = sid + "-output-" + date.strftime("%Y%m%d") + ".is." + args.outputformat
        outputfilename = os.path.join(self.sid_path(sid), filename)

        if os.path.isfile(outputfilename) and not args.overwrite:
            raise ValueError("{}: Outputfile already exists at {}, use --overwrite.".format(sid, outputfilename))

        return outputfilename

    def inputfilename(self, sid):
        """
        Return the path to the expected input filename for a given sid.
        """
        args = self.parse_args()
        date = datetime.date.today()

        if args.interval == "monthly":
            date = monthly()
        if args.interval == "weekly":
            date = weekly()

        filename = sid + "-input-" + date.strftime("%Y%m%d") + "." + args.inputformat
        inputfilename = os.path.join(self.sid_path(sid), filename)

        return inputfilename
