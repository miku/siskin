# coding: utf-8
# pylint: disable=C0301,E1101

# Copyright 2019 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
#                   Martin Czygan, <martin.czygan@uni-leipzig.de>
#                   Robert Schenk, <robert.schenk@uni-leipzig.de>
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
KXP, FID-BBI

Tickets: 15833, 15694
"""

import datetime

import luigi
from gluish.common import Executable
from gluish.intervals import weekly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask


class KXPTask(DefaultTask):
    """
    Base task for KXP. TAG might change from kxp to numeric id in the future.
    """
    TAG = 'kxp'

    def closest(self):
        return weekly(date=self.date)


class KXPSRU(KXPTask):
    """
    Fetch selection via SRU.
    """
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return Executable(name='srufetch', message='https://github.com/miku/srufetch')

    def run(self):
        # XXX: Move this to config, then gitlab.
        selector = """
            pica.bkl="05.15" or pica.bkl="05.30" or pica.bkl="05.33" or
            pica.bkl="05.38" or pica.bkl="05.39" or pica.bkl="06.*" or
            pica.bkl="17.80" or pica.bkl="17.91" or pica.bkl="17.94" or
            pica.bkl="21.30" or pica.bkl="21.31" or pica.bkl="21.32" or
            pica.bkl="21.93" or pica.bkl="53.70" or pica.bkl="53.71" or
            pica.bkl="53.72" or pica.bkl="53.73" or pica.bkl="53.74" or
            pica.bkl="53.75" or pica.bkl="53.76" or pica.bkl="53.82" or
            pica.bkl="53.89" or pica.bkl="54.08" or pica.bkl="54.10" or
            pica.bkl="54.22" or pica.bkl="54.32" or pica.bkl="54.38" or
            pica.bkl="54.51" or pica.bkl="54.52" or pica.bkl="54.53" or
            pica.bkl="54.54" or pica.bkl="54.55" or pica.bkl="54.62" or
            pica.bkl="54.64" or pica.bkl="54.65" or pica.bkl="54.72" or
            pica.bkl="54.73" or pica.bkl="54.74" or pica.bkl="54.75" or
            pica.bkl="86.28" or pica.ssg="24,1" or pica.ssg="bbi" or
            pica.sfk="bub" or pica.osg="bbi" or pica.rvk="AM 1*" or
            pica.rvk="AM 2*" or pica.rvk="AM 3*" or pica.rvk="AM 4*" or
            pica.rvk="AM 5*" or pica.rvk="AM 6*" or pica.rvk="AM 7*" or
            pica.rvk="AM 8*" or pica.rvk="AM 9*" or pica.rvk="AN 1*" or
            pica.rvk="AN 2*" or pica.rvk="AN 3*" or pica.rvk="AN 4*" or
            pica.rvk="AN 5*" or pica.rvk="AN 6*" or pica.rvk="AN 7*" or
            pica.rvk="AN 8*" or pica.rvk="AN 9*" or pica.rvk="AP 25*" or
            pica.rvk="AP 28*" or pica.rvk="LH 72*" or pica.rvk="LK 86*" or
            pica.rvk="LR 5570*" or pica.rvk="LR 5580*" or pica.rvk="LR 5581*"
            or pica.rvk="LR 5582*" or pica.rvk="LR 5586*" or pica.rvk="LR
            57240" or pica.rvk="LT 5570*" or pica.rvk="LT 5580*" or
            pica.rvk="LT 5581*" or pica.rvk="LT 5582*" or pica.rvk="LT 5586*"
            or pica.rvk="LT 57240" or pica.sbn="vd17" or pica.sbn="vd18"
        """
        output = shellout("""srufetch -x -q '{selector}' > {output} """, selector=selector)
        output = shellout("""yaz-marcdump -i marcxml -o marc {input} > {output}""", input=output)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext="mrc"))
