#!/usr/bin/env python
# coding: utf-8

"""
Task can override complete().

Goal:

* Flexibility wrt. completion criteria.

Run:

    $ python x02.py FreeSpace
    ...
    This progress looks :)
    ...

    $ python x02.py FreeSpace --free 80000
    ...

    RuntimeError: 80000 MB of free space required
    ...

"""

from __future__ import print_function
import luigi
import os


class FreeSpace(luigi.Task):
    """
    A custom completion criteria. Here, we want to ensure, that there is
    enough room on.
    """
    free = luigi.IntParameter(description='in megabytes', default=100)

    def requires(self):
        pass

    def run(self):
        raise RuntimeError('%s MB of free space required' % self.free)

    def complete(self):
        statvfs = os.statvfs('/')
        required, free = self.free * 1048576, statvfs.f_frsize * statvfs.f_bavail
        return free > required

if __name__ == '__main__':
    luigi.run(local_scheduler=True)
