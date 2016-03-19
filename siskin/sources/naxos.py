# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301
#
#  Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                    The Finc Authors, http://finc.info
#                    Martin Czygan, <martin.czygan@uni-leipzig.de>
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

"""
NAXOS.

Configuration keys:

[naxos]

ftp-host = host.name
ftp-username = username
ftp-password = password
ftp-filepath = /path/to/marc-nml-gesamt.zip
"""

from __future__ import print_function
from siskin.benchmark import timed
from siskin.common import FTPFile
from luigi.contrib.esindex import CopyToIndex
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.configuration import Config
from siskin.task import DefaultTask
import datetime
import luigi

config = Config.instance()

class NaxosTask(DefaultTask):
    TAG = '005'

    def closest(self):
        return datetime.date(2014, 1, 1)

class NaxosJson(NaxosTask):
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return FTPFile(host=config.get('naxos', 'ftp-host'),
                       username=config.get('naxos', 'ftp-username'),
                       password=config.get('naxos', 'ftp-password'),
                       filepath=config.get('naxos', 'ftp-filepath'))

    @timed
    def run(self):
        """ Unzip and strip namespace. """
        output = shellout("""unzip -p {input} {path} |
                             LANG=C perl -lnpe 's@xmlns:marc="http://www.loc.gov/MARC21/slim"@@g' |
                             LANG=C perl -lnpe 's@<marc:@<@' |
                             LANG=C perl -lnpe 's@</marc:@</@' > {output}""",
                          input=self.input().path, path='marc-nml-gesamt.xml')
        output = shellout("marcxmltojson -m date={date} {input} > {output}", date=self.closest(), input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class NaxosIndex(NaxosTask, CopyToIndex):
    date = ClosestDateParameter(default=datetime.date.today())

    index = 'naxos'
    doc_type = 'title'
    purge_existing_index = True

    settings = {
        "settings": {
            "number_of_shards": 5,
            "analysis": {
                "filter": {
                    "autocomplete_filter": {
                        "type":     "edge_ngram",
                        "min_gram": 1,
                        "max_gram": 20
                    }
                },
                "analyzer": {
                    "autocomplete": {
                        "type":      "custom",
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "autocomplete_filter"
                        ]
                    }
                }
            }
        }
    }

    mapping = {
        'title': {
            'date_detection': False,
            '_id': {
                'path': 'content.001'
            },
            '_all': {
                'enabled': True,
                'term_vector': 'with_positions_offsets',
                'store': True
            },
            'properties': {
                'content': {
                    'properties': {
                        '245': {
                            'properties': {
                                'a': {
                                    'type': 'string',
                                    'analyzer': 'autocomplete'
                                }
                            }
                        },
                        '100': {
                            'properties': {
                                'a': {
                                    'type': 'string',
                                    'analyzer': 'autocomplete'
                                }
                            }
                        },
                        '700': {
                            'properties': {
                                'a': {
                                    'type': 'string',
                                    'analyzer': 'autocomplete'
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    def update_id(self):
        """ This id will be a unique identifier for this indexing task."""
        return self.effective_task_id()

    def requires(self):
        return NaxosJson(date=self.date)
