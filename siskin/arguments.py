import argparse
import datetime
import os.path
import pymarc
import sys

from siskin.configuration import Config
from gluish.intervals import monthly, weekly
from gluish.parameter import ClosestDateParameter


def get_arguments():

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-i", "--input", dest="inputfilename", help="inputfile")
    parser.add_argument("--filemap", dest="filemap", help="path of the file containing a dictionary, list or another map")
    parser.add_argument("--overwrite", dest="overwrite", help="overwrite existing outputfile", nargs="?", const=True, default=False)
    parser.add_argument("--format", dest="outputformat", help="outputformat mrc or xml", default="mrc")
    parser.add_argument("--interval", dest="interval", help="interval for update", default="monthly")
    parser.add_argument("--root", dest="root", help="root path for all data")

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
    outputfilename = SID + "-output-" + date + "." + outputformat
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
