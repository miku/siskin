# coding: utf-8

import luigi

class ILNParameter(luigi.Parameter):
    """
    Parse ILN (internal library number), so that the result is always in the
    *4 char* format.
    """
    def parse(self, s):
        """ Parse the value and pad left with zeroes. """
        return s.zfill(4)