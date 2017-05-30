#!/usr/bin/env python
# coding: utf-8

"""
An example task.

First, start scheduler in the background.

    $ luigid --background

Goal:

* Show web interface.

Simple command line integration.

    $ python x01.py MyTask

Screenies:

* http://i.imgur.com/OTgMTZP.png

"""

import time

import luigi


class MyTask(luigi.Task):
    """ Simulate work, so we can inspect the web interface. """

    def requires(self):
        pass

    def run(self):
        """ TODO: Adjust HTTP URL. """
        print('running task (may want to visit http://127.0.0.1:8082)')
        time.sleep(30)
        with self.output().open('w') as output:
            output.write('Some bytes written to a file.\n')

    def output(self):
        return luigi.LocalTarget(path='outputs/x01.txt')

if __name__ == '__main__':
    luigi.run()
