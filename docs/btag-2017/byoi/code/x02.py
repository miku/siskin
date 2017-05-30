#!/usr/bin/env python
# coding: utf-8

"""
Task can override complete().

Goal:

* Flexibility wrt. completion criteria.

Run:

    $ python x02.py MyTask
    ...
    This progress looks :)
    ...

    $ python x02.py FreeSpace --free 800
    ...

    RuntimeError: 800 GB of free space required
    ...

"""

from __future__ import print_function

import os

import luigi


class MyTask(luigi.Task):
    """
    A custom completion criteria. Here, we want to ensure, that there is
    enough room on.
    """
    free = luigi.IntParameter(description='in GB', default=100)

    def requires(self):
        pass

    def run(self):
	raise RuntimeError('%s GB of free space required' % self.free)

    def complete(self):
        statvfs = os.statvfs('/')
	required, free = self.free * 1073741824, statvfs.f_frsize * statvfs.f_bavail
        return free > required

if __name__ == '__main__':
    luigi.run(local_scheduler=True)
