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
import os.path
import pymarc
import glob
import re
import sys

from siskin.configuration import Config
from gluish.intervals import monthly, weekly
from gluish.parameter import ClosestDateParameter


def get_arguments():
    """
    Return default set of arguments for lightweight scripts.
    """
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-i", "--input", dest="inputfilename", help="inputfile")
    parser.add_argument("--filemap", dest="filemap", help="path of the file containing a dictionary, list or another map")
    parser.add_argument("--overwrite", dest="overwrite", help="overwrite existing outputfile", nargs="?", const=True, default=False)
    parser.add_argument("--format", dest="outputformat", help="outputformat mrc or xml", default="mrc")
    parser.add_argument("--interval", dest="interval", help="interval for update", default="monthly")
    parser.add_argument("--root", dest="root", help="root path for all data")
    parser.add_argument("--output-history", dest="output_history", help="number of older outputfiles to keep", default="3")
    parser.add_argument("--input-history", dest="input_history", help="number of older inputfiles to keep", default="3")

    args = parser.parse_args()
    return args


def get_path(args, SID):

    root = args.root

    if not root:
        config = Config.instance()
        try:
            root = config.get("core", "home")
        except:
            root = ""

    if not root:
        sys.exit(SID + ": No root path for data given. Use --root or specify a default root in the siskin.ini configuration file.")

    if os.path.isdir(root):
        path = os.path.join(root, SID)
        if not os.path.isdir(path):
            os.mkdir(path)
    else:
        sys.exit(SID + ": Root path does not exists: " + root)

    return path


def remove_old_inputfiles(args, SID):

    n = int(args.input_history)
    inputfilename = build_inputfilename(args, SID)
    inputfilenames_to_delete = re.sub("-input-(.*)", "*", inputfilename)

    files = glob.glob(inputfilenames_to_delete)
    files = sorted(files, key=os.path.getctime)
    for filename in files[:-n]:
        os.remove(filename)


def remove_old_outputfiles(args, SID):

    n = int(args.output_history)
    outputfilename = build_outputfilename(args, SID)
    outputfilenames_to_delete = re.sub("-output-(.*)", "*", outputfilename)

    files = glob.glob(outputfilenames_to_delete)
    files = sorted(files, key=os.path.getctime)
    for filename in files[:-n]:
        os.remove(filename)


def build_outputfilename(args, SID):

    overwrite = args.overwrite
    outputformat = args.outputformat
    interval = args.interval
    path = get_path(args, SID)

    # Check outputformat
    if outputformat != "mrc" and outputformat != "xml":
        sys.exit(SID + ": Unsupported format. Choose mrc or xml.")

    # Check interval
    if interval not in ("monthly", "weekly", "daily", "manually"):
        sys.exit(SID + ": Unsupported interval. Choose monthly, weekly, daily or manually.")

    if interval == "manually" and not overwrite:
        sys.exit(SID + ": Interval is manually. Use --overwrite to force a new output.")

    # Set closest date
    today = datetime.date.today()
    if interval == "monthly":
        date = monthly(today)
    elif interval == "weekly":
        date = weekly(today)
    elif interval == "daily" or interval == "manually":
        date = today

    # Set outputfilename
    date = date.strftime("%Y%m%d")
    outputfilename = SID + "-output-" + date + ".fincmarc." + outputformat
    outputfilename = os.path.join(path, outputfilename)

    # Check if current output already exist
    if os.path.isfile(outputfilename) and not overwrite:
        sys.exit(SID + ": Outputfile already exists. Use --overwrite.")

    return outputfilename


def build_inputfilename(args, inputformat, SID):

    interval = args.interval
    path = get_path(args, SID)

    # Set closest date
    today = datetime.date.today()
    if interval == "monthly":
        date = monthly(today)
    elif interval == "weekly":
        date = weekly(today)
    elif interval == "daily" or interval == "manually":
        date = today

    # Set inputfilename
    date = date.strftime("%Y%m%d")
    inputfilename = SID + "-input-" + date + "." + inputformat
    inputfilename = os.path.join(path, inputfilename)

    return inputfilename
