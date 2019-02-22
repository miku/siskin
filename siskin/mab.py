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

MAB specification: https://www.dnb.de/DE/Standardisierung/Formate/MAB/mab_node.html

Example MABXML files: https://git.io/fhFHr.

    from siskin.mab import MabXMLFile

    mf = MabXMLFile("fixtures/mab0.xml")
    for record in mf.records():

        # Return the first value or None.
        title = record.field("331")
        if not title:
            raise ValueError("record has not title")

        # Return multiple values.
        for isbn in record.fields("540"):
            print(isbn)

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

    def feld(self, number, code=None, alt=None):
        """
        Shortcut, since MAB is a German format.
        """
        return self.field(number, code=code, alt=None)

    def felder(self, number, code=None):
        """
        Shortcut, since MAB is a German format.
        """
        return self.fields(number, code=code)

    def field(self, number, code=None, alt=None):
        """
        Returns the value of the first (only) field matching number or None.
        """
        for f in self.dd.get("feld", []):
            if f.get("@nr") == number:
                if code:
                    for sf in f.get("uf", []):
                        if sf.get("@code") == code:
                            return sf.get("#text", alt)
                else:
                    return f.get("#text", alt)
        return alt

    def fields(self, number, code=None):
        """
        Returns the values of the all fields matching number and possibly
        subfield or empty list.
        """
        result = []

        for f in self.dd.get("feld", []):
            if f.get("@nr") != number:
                continue
            if code:
                for sf in f.get("uf", []):
                    if sf.get("@code") != code:
                        continue
                    if sf.get("#text"):
                        result.append(sf.get("#text"))
            else:
                if f.get("#text"):
                    result.append(f.get("#text"))

        return result

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
        Eagerly parse the XML from filename into a xmltodict defaultdict.
        TODO(miku): Use streaming mode.
        """
        self.filename = filename
        with open(self.filename) as handle:
            handle = handle.read().replace("¬", "")
            self.dd = xmltodict.parse(handle, force_list=('datensatz', 'feld', 'uf'))
        if not "datei" in self.dd:
            raise ValueError("datei tag not found")

    def records(self):
        """
        Returns a generator over all mab records (datensatz). TODO(miku): Use
        iteration protocol.
        """
        for ds in self.dd["datei"].get("datensatz", []):
            yield MabRecord(ds)

    def __str__(self):
        return '<MabXMLFile filename={}>'.format(self.filename)

    def __repr__(self):
        return self.__str__()
