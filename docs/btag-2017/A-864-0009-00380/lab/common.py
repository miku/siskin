# coding: utf-8

from __future__ import print_function

import os

import luigi

from gluish.task import BaseTask

__dir__ = os.path.dirname(os.path.realpath(__file__))


class DefaultTask(BaseTask):
    """
    A base task that sets an output directory for all task outputs.
    """
    BASE = os.path.join(__dir__, '../outputs')

    def assets(self, path):
	""" Return the absolute path for an asset. """
	return os.path.join(__dir__, '../assets', path)
