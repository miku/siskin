#!/usr/bin/env python
# coding: utf-8

"""
Custom completion criteria
==========================

Goals:

* show flexibility with respect to completion criteria

----

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

import os

import luigi


class MyTask(luigi.Task):
    """
    A custom completion criteria. Here, we want to ensure, that there is enough
    space on the device.
    """

    free = luigi.IntParameter(description="in GB", default=100)

    def run(self):
        raise RuntimeError("%s GB of free space required" % self.free)

    def complete(self):
        statvfs = os.statvfs("/")
        required, free = self.free * 1073741824, statvfs.f_frsize * statvfs.f_bavail
        return free > required


if __name__ == "__main__":
    luigi.run(local_scheduler=True)
