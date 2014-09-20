# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103

"""
Hackernews stories and comments.
"""

from gluish.utils import shellout
from siskin.task import DefaultTask
import luigi

class HNTask(DefaultTask):
    TAG = 'hackernews'

class HNCommentsDownload(HNTask):
    """ Download comments. """

    def run(self):
	url = "https://ia902503.us.archive.org/33/items/HackerNewsStoriesAndCommentsDump/HNCommentsAll.7z"
	output = shellout("wget -O {output} {url}", url=url)
	luigi.File(output).move(self.output().path)

    def output(self):
	return luigi.LocalTarget(path=self.path(ext='7z'))
