# coding: utf-8
# pylint: disable=C0301,E1101

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

"""
ZDB, for matching short titles of journals.

See: goo.gl/90cteW
"""

import os

import luigi
from gluish.format import Gzip
from gluish.utils import shellout

from siskin.task import DefaultTask


class ZDBTask(DefaultTask):
    """
    ZDB base task.
    """
    TASK = 'zdb'


class ZDBDownload(ZDBTask):
    """
    Download snapshot.
    """
    format = luigi.Parameter(default="jsonld", description="rdf, hdt")

    def run(self):
        link = "http://datendienst.dnb.de/cgi-bin/mabit.pl?cmd=fetch&userID=opendata&pass=opendata&mabheft=ZDBTitel.%s.gz" % self.format
        output = shellout(""" wget -O {output} "{link}" """, link=link)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='jsonld.gz'), format=Gzip)


class ZDBShortTitleMap(ZDBTask):
    """
    Just an ad-hoc task with some ad-hoc code, refs #10562.
    """

    def requires(self):
        return ZDBDownload(format="rdf")

    def run(self):
        """
        @IHBhY2thZ2UgbWFpbgoK@IGltcG9ydCAoCiAg@@ImVuY29kaW5nL2pzb24iCiAg@@ImVuY29kaW5nL3htbCIK@@IC
        AibG9nIgog@@ICJvcyIKCiAg@@InN0cmluZ3MiCgog@@ICJnaXRodWIuY29tL21pa3UveG1sc3RyZWFtIgog@KQoK@
        IHR5cGUgRGVzY3JpcHRpb24gc3RydWN0IHsK@@ICBYTUxOYW1l@IHhtbC5OYW1lIGB4bWw6IkRlc2NyaXB0aW9uImA
        K@@ICBJc3Nu@@IFtdc3RyaW5nIGB4bWw6Imlzc24iYAog@@IFNob3J0VGl0bGUgW11zdHJpbmcgYHhtbDoic2hvcnR
        UaXRsZSJgCiAg@@VGl0bGUg@ICBbXXN0cmluZyBgeG1sOiJ0aXRsZSJgCiAgICB9Cgog@ZnVuYyBtYWluKCkgewog@
        @IHNtIDo9IG1ha2UobWFwW3N0cmluZ11zdHJpbmcpCiAg@@c2Nhbm5lciA6PSB4bWxzdHJlYW0uTmV3U2Nhbm5lcih
        vcy5TdGRpbiwgbmV3KERlc2NyaXB0aW9uKSkK@@ICBmb3Igc2Nhbm5lci5TY2FuKCkgewog@@@ICB0YWcgOj0gc2Nh
        bm5lci5FbGVtZW50KCkK@@@@aWYgdiwgb2sgOj0gdGFnLigqRGVzY3JpcHRpb24pOyBvayB7CiAg@@@@ICBpZiBsZW
        4odi5TaG9ydFRpdGxlKSA9PSAwIHsK@@@@@@ICBjb250aW51ZQog@@@@@fQog@@@@@Zm9yIF8sIHMgOj0gcmFuZ2Ug
        di5TaG9ydFRpdGxlIHsK@@@@@@ICBmb3IgXywgdCA6PSByYW5nZSB2LlRpdGxlIHsK@@@@@@@@c21bc10gPSB0CiAg
        @@@@@@@IHNtW3N0cmluZ3MuVG9Mb3dlcihzKV0gPSB0CiAg@@@@@@fQog@@@@@fQog@@@ICB9CiAg@@fQog@@IGlmI
        GVyciA6PSBzY2FubmVyLkVycigpOyBlcnIgIT0gbmlsIHsK@@@@bG9nLkZhdGFsKGVycikK@@ICB9CiAg@@aWYgZXJ
        yIDo9IGpzb24uTmV3RW5jb2Rlcihvcy5TdGRvdXQpLkVuY29kZShzbSk7IGVyciAhPSBuaWwgewog@@@ICBsb2cuRm
        F0YWwoZXJyKQog@@IH0K@IH0K
        """
        source = self.run.__doc__.replace("\n", "").replace(" ", "").replace("@", "ICAg")
        tempcode = shellout("""echo '{code}' | base64 -d > {output}.go """, code=source, preserve_whitespace=True)
        output = shellout(""" unpigz -c {input} | go run {code}.go > {output} """, code=tempcode, input=self.input().path)
        os.remove(tempcode)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='json'))
