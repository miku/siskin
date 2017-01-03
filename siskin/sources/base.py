# coding: utf-8
# pylint: disable=C0301,E1101

# Copyright 2016 by Leipzig University Library, http://ub.uni-leipzig.de
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

"""
BASE.

* https://www.base-search.net/
* https://api.base-search.net/
* https://en.wikipedia.org/wiki/BASE_(search_engine)
* http://www.dlib.org/dlib/june04/lossau/06lossau.html

"""

import datetime

import luigi

from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.common import Executable
from siskin.task import DefaultTask


class BaseTask(DefaultTask):
    """
    Various tasks around base.
    """
    TAG = 'base'

    def closest(self):
        return weekly(self.date)


class BaseDynamicSet(BaseTask):
    """
    TODO(miku): Dynamic sets use SOLR queries:

    set=classcode:(002 OR 070 OR 659 OR 791)     => fq on SORL field classcode
    set=subject:(*journalismus* OR *journalism*) => fq on SOLR field subject
    set=classcode:(002 OR 070 OR 659 OR 791) subject:(*journalismus* OR *journalism*) => fq on SOLR field classcode and subject

    Long set names currently cause problems with metha-sync.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    prefix = luigi.Parameter(default="oai_dc", significant=False)
    set = luigi.Parameter(description='a subject / keyword')

    def requires(self):
        return Executable(name='metha-sync', message='https://github.com/miku/metha')

    def run(self):
        shellout("METHA_DIR={dir} metha-sync -set '{set}' -format {prefix} http://oai.base-search.net/oai",
                 set=self.set, prefix=self.prefix, dir=self.config.get('core', 'metha-dir'))
        output = shellout("METHA_DIR={dir} metha-cat -set '{set}' -format {prefix} http://oai.base-search.net/oai | pigz -c > {output}",
                          set=self.set, prefix=self.prefix, dir=self.config.get('core', 'metha-dir'))
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="xml.gz", digest=True))
