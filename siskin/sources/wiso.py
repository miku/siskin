# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904,E1103,C0301

# Copyright 2017 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
#                   Martin Czygan, <martin.czygan@uni-leipzig.de>
#
# This file is part of some open source application.
#
# Some open source application is free software: you can redistribute
# it and/or modify it under the terms of the GNU General Public
# License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.
#
# Some open source application is distributed in the hope that it will
# be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
#
# @license GPL-3.0+ <http://spdx.org/licenses/GPL-3.0+>
#

"""
Wiso aux tasks.

Config
------

[redmine]
baseurl = https://example.projects.de
apikey = d24411122381317432647264876234

"""

import luigi
import datetime
import tempfile
import os
from gluish.utils import shellout
from siskin.task import DefaultTask
from siskin.common import RedmineDownloadAttachments


class WisoTask(DefaultTask):
    """
    An auxiliary namespace for WISO related tasks, refs #12301.
    """
    TAG = 'wiso'


class Wiso2018Files(WisoTask):
    """
    Combine into single file.

    >>> import pandas as pd
    >>> df = pd.read_csv("output.csv", sep=";", encoding='latin1')
    >>> df.columns
    Index(['wiso-net_FZS_Psychologie_2018.csv', 'Zeitschrift', 'Regionalausgaben',
        'Sprache', 'Verlag', 'ISSN', '2. ISSN', 'Beginn Volltextarchiv (Datum)',
        'Ende Volltextarchiv (Datum)', 'Beginn Volltextarchiv (Heft)',
        'Beginn Volltextarchiv (Jahr)', 'Ende Volltextarchiv (Heft)',
        'Ende Volltextarchiv (Jahr)', 'Verzug', 'URL Inhaltsverzeichnis',
        'URL Artikelebene', 'URL Publikation', 'Schlagworte', 'Bemerkungen',
        'Aktualisierung', 'DB-Kürzel', 'Produktsigel'],
        dtype='object')

    """

    def requires(self):
        return RedmineDownloadAttachments(issue="12301")

    def run(self):
        with self.input().open() as handle:
            _, combined = tempfile.mkstemp(prefix='siskin-')
            for row in handle.iter_tsv(cols=('path',)):
                # Read CSV file and prepend the basename of the file to the line.
                filename = os.path.basename(row.path)
                shellout(""" awk '{{print "{filename};"$0}}' < {input} >> {output}""",
                         filename=filename, input=row.path, output=combined)

        luigi.LocalTarget(combined).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='csv'))
