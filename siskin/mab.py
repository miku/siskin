# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301,C0103,W0614,W0401,E0202

# Copyright 2019 by Leipzig University Library, http://ub.uni-leipzig.de
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
Slight MABXML abstraction layer, using xmltodict under the hood. Usable up to
file sizes of 50M. TODO: For larger files, rewrite this with streaming.

<datei xmlns="http://www.ddb.de/professionell/mabxml/mabxml-1.xsd" ... >
  <datensatz xmlns="http://www.ddb.de/professionell/mabxml/mabxml-1.xsd" typ="u" status="n" mabVersion="M2.0">
    <feld nr="001" ind=" ">10598985</feld>
    <feld nr="002" ind="a">20070926</feld>
    <feld nr="003" ind=" ">20160311</feld>
    <feld nr="004" ind=" ">20190110</feld>
    <feld nr="010" ind=" ">10390691</feld>
    <feld nr="020" ind="b">ITKGDK500164319</feld>
    <feld nr="030" ind=" ">e|5|xr|zc||||</feld>
    <feld nr="050" ind=" ">a|a|||||||||||</feld>
    <feld nr="051" ind=" ">s|||||||</feld>
    <feld nr="070" ind="a">GDK</feld>
    <feld nr="071" ind=" ">80</feld>
    <feld nr="433" ind=" ">[ca. 300  Seiten ]</feld>
    <feld nr="501" ind=" ">Enthält die Berichte über Aktivitäten im Jahr 2006</feld>
  </datensatz>
</datei>

"""

import xmltodict

class MabRecord(object):
    """
    A single MAB record.
    """
    def __init__(self, dd):
        """
        Initialize with the default dictionary created by xmltodict for a
        single datensatz element.
        """
        self.dd = dd

    def status(self):
        return self.dd.get("@status")

    def typ(self):
        return self.dd.get("@typ")

    def mabVersion(self):
        return self.dd.get("@mabVersion")

    def feld(self, number, code=None):
        return self.field(number, code=code)

    def field(self, number, code=None):
        """
        Returns the value of the first (only) field matching number or None.
        """
        for f in self.dd.get("feld", []):
            if f.get("@nr") == number:
                if code:
                    for sf in f.get("uf", []):
                        if sf.get("@code") == code:
                            return sf.get("#text")
                else:
                    return f.get("#text")

    def size(self):
        return len([_ for _ in self.dd.get("feld", [])])

    def __str__(self):
        return '<MabXMLRecord status=%s, typ=%s, with %s fields>' % (self.status(), self.typ(), self.size())

    def __repr__(self):
        return self.__str__()

class MabXMLFile(object):
    """
    Encapsulate MAB XML methods.
    """
    def __init__(self, filename):
        """
        Eagerly load the file and puts content into a defaultdict.
        """
        self.filename = filename
        with open(self.filename) as handle:
            self.dd = xmltodict.parse(handle.read(), force_list=('datensatz', 'feld', 'uf'))
        if not "datei" in self.dd:
            raise ValueError("datei tag not found")

    def records(self):
        for ds in self.dd["datei"].get("datensatz", []):
            yield MabRecord(ds)

    def __str__(self):
        return '<MabXMLFile filename={}>'.format(self.filename)

    def __repr__(self):
        return self.__str__()
