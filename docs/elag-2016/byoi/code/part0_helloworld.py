#!/usr/bin/env python

"""
Part 0
======

Hello World.

----

To run:

    (vm) $ python part0_helloworld.py

Run with less noise:

    (vm) $ python part0_helloworld.py 2> /dev/null

"""

import luigi


class HelloWorldTask(luigi.Task):
    def run(self):
        print("{task} says: Hello world!".format(task=self.__class__.__name__))


if __name__ == "__main__":
    luigi.run(["HelloWorldTask", "--workers", "1", "--local-scheduler"])
