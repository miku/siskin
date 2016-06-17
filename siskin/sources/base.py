# coding: utf-8
# pylint: disable=C0301

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
import tempfile

import luigi
from gluish.interval import weekly
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

    date = ClosestDateParameter(default=datetime.date.today())
    prefix = luigi.Parameter(default="oai_dc", significant=False)
    set = luigi.Parameter(description='a subject / keyword')

    def requires(self):
        return Executable(name='metha-sync', message='https://github.com/miku/metha')

    def run(self):
        shellout("METHA_DIR={dir} metha-sync -set '{set}' -format {prefix} http://oai.base-search.net/oai",
                 set=self.set, prefix=self.prefix, dir=self.config.get('core', 'metha-dir'))
        output = shellout("METHA_DIR={dir} metha-cat -set '{set}' -format {prefix} http://oai.base-search.net/oai | pigz -c > {output}",
                          set=self.set, prefix=self.prefix, dir=self.config.get('core', 'metha-dir'), pipefail=True)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="xml.gz", digest=True))

class BaseFIDSample(BaseTask):
    """
    Base subject filtered. Most probably the query results overlap.

    http://oai.base-search.net/index.html#overview-of-indexed-fields
    """
    date = ClosestDateParameter(default=datetime.date.today())
    prefix = luigi.Parameter(default="oai_dc", significant=False)

    def requires(self):
        """
        TODO(miku): Is there a way to OR various queries? No example in the
        documentation (http://oai.base-search.net/index.html#dynamic-sets).
        """
        return [
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"cinema"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"crisis%20communication"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"*journalismus*"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"*journalism*"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"mediensystem*"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"kommunikationswissenschaft*"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"buchwissenschaft*"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"chinese%20communication"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"climate%20communication"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"media%20studies"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"communication%20media%20studies"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"communication%20studies"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"communication%20management"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"corporate%20communication"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"crisis%20communication"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"empirische%20kommunikationsforschung"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"empirische%20medienforschung"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"environmental%20communication"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"fernseh*"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"filmwissenschaft*"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"film%20studies"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"film%20music"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"filmmusik"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"cinema"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"health%20communication"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"intermedial*"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"kommunikationsforschung"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"kommunikationsgeschichte"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"medienp%E4dagogik"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"literaturverfilmung"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"mass%20communication"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"massenkommunikation"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"mass%20media"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"massenmedien"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"zeitung"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"tagespresse"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"spielfilm"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"dokumentarfilm"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"tageszeitung"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"kurzfilm"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"feature%20film"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"documentary%20film"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"short%20film"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"media%20pedagogy"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"media%20psychology"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"medienpsychologie"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"media%20theory"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"medientheorie"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"kommunikationstheorie"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"medienkultur*"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"mediensoziologie"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"musicalfilm"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"public%20relations"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"political%20communication"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"politische%20kommunikation"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"printjournalism*"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"publizistik"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"social%20media"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"sportjournalism*"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"wissenschaftskommunikation"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"communication%20theory"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"cultural%20communication"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"game%20studies"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"journalism%20studies"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"journalism%20history"'),
            BaseDynamicSet(date=self.date, prefix=self.prefix, set='subject:"media%20history"'),
        ]

    def run(self):
        _, output = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path, output=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="xml.gz", digest=True))
