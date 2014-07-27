# coding: utf-8

"""
Support for importing all workflows at once.
"""

import os
import glob

modules = glob.glob("%s/*.py" % (os.path.dirname(__file__)))
__all__ = [os.path.basename(f)[:-3] for f in modules]
