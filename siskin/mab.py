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

    mabfile = MabXMLFile("fixtures/mab0.xml")

    for record in mabfile:

        # Return the first value or None.
        title = record.field("331")
        if not title:
            raise ValueError("record has not title")

        # Return multiple values (or empty list).
        for isbn in record.fields("540"):
            print(isbn)

"""

import io
import os
import six
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
    def __init__(self, data, replace=None, encoding='utf-8'):
        """
        Eagerly parse the XML from filename, filelike or string into a
        xmltodict defaultdict. Basic checks for MAB like XML.

        TODO(miku): Use streaming mode.

        The replace optional argument should be a list of desired substitutions
        in the content, e.g.  (("¬", ""),) to be applied before parsing.
        """
        if isinstance(data, six.string_types):
            if os.path.exists(data):
                # Assume it is a file.
                with io.open(data, encoding=encoding) as handle:
                    content = handle.read()
            else:
                # Assume direct data.
                content = data
        else:
            # Assume something that can be read from.
            content = data.read()

        if replace:
            for value, replacement in replace:
                content = content.replace(value, replacement)

        self.dd = xmltodict.parse(content, force_list=('datensatz', 'feld', 'uf'))
        if not "datei" in self.dd:
            raise ValueError("datei tag not found or empty file without fields")

        # It there are no fields, we get an error.
        if self.dd["datei"] is None:
            self.records = []
        else:
            self.records = self.dd["datei"].get("datensatz", [])

    def __iter__(self):
        return self

    def __next__(self):
        """
        Temporary attribute for iteration.
        """
        if not hasattr(self, "_it"):
            self._it = iter(self.records)
        try:
            # http://diveinto.org/python3/porting-code-to-python-3-with-2to3.html#next
            if hasattr(self._it, "next"):
                return MabRecord(self._it.next())
            return MabRecord(next(self._it))
        except StopIteration:
            delattr(self, "_it")
            raise

    def next(self):
        """
        Python 2 compatibility.
        """
        return self.__next__()

    def __str__(self):
        return '<MabXMLFile>'

    def __repr__(self):
        return self.__str__()
