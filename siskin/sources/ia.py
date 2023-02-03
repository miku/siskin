#!/usr/bin/env python
# coding: utf-8
"""
Data hosted at Internet Archive.
"""

import datetime
import os
import shutil
import tempfile
import luigi

from siskin.task import DefaultTask
from gluish.utils import shellout


class IATask(DefaultTask):
    """
    Base task for IA related tasks.
    """

    TAG = "ia"


class DownloadFile(IATask):
    """
    Fetch a file.
    """

    itempath = luigi.Parameter(
        default="fatcat_bulk_exports_2022-11-24/release_extid.tsv.gz"
    )

    def run(self):
        tmpdir = tempfile.mkdtemp("siskin-", "", "")
        shellout(
            "rclone copy ia:/{itempath} {tmpdir}", item=self.itempath, tmpdir=tmpdir
        )
        src = os.path.join(tmpdir, os.path.basename(self.itempath))
        luigi.LocalTarget(src).move(self.output().path)

    def output(self):
        path = os.path.join(self.taskdir(), self.itempath)
        return luigi.LocalTarget(path=path)
