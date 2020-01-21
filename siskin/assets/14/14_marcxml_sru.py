#!/usr/bin/env python
# coding: utf-8
#
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

# Source: Répertoire International des Sources Musicales (RISM)
# SID: 14
# Ticket: #16061, #16186

"""

import os
import re
import sys
from io import BytesIO, StringIO

import requests

import marcx
import pymarc
from siskin.mappings import formats
from siskin.utils import marc_clean_record
from six.moves.urllib.parse import urlencode


def has_digitalization_links(newrecord):
    for field_856 in newrecord.get_fields("856"):
        for x in field_856.get_subfields("x"):
            if "Digitalization" == x:
                return True
        for z in field_856.get_subfields("z"):
            if "digit" in z or "Digit" in z and not ("Bach digital" in z or "Bach Digital" in z):
                return True
            if "exemplar" in z or "Exemplar" in z:
                return True


def get_digitalization_links(newrecord):
    """
    Given a marc record, just return all links (from 856.u).
    """
    for field_856 in newrecord.get_fields("856"):
        is_digitalization = False
        for x in field_856.get_subfields("x"):
            if "Digitalization" == x:
                is_digitalization = True
        if not is_digitalization:
            for z in field_856.get_subfields("z"):
                if "digit" in z or "Digit" in z and not ("Bach digital" in z or "Bach Digital" in z):
                    is_digitalization = True
                elif "exemplar" in z or "Exemplar" in z:
                    is_digitalization = True
        if is_digitalization:
            for url in field_856.get_subfields("u"):
                yield url


def get_sru_records(start, max):

    params = {"maximumRecords": max, "startRecord": start, "recordSchema": "marc", "operation": "searchRetrieve", "query": '*'}

    params = urlencode(params)
    result = requests.get("https://muscat.rism.info/sru?%s" % params)
    result = result.text
    result = result.replace("\n", "")
    result = result.replace("\t", "")
    result = result.replace("</marc:record>", "</marc:record>\n")
    result = result.replace("<marc:", "<")
    result = result.replace("/marc:", "/")
    return result.split("\n")


outputfilename = "14_output.xml"

if len(sys.argv) == 2:
    outputfilename = sys.argv[1:]

writer = pymarc.XMLWriter(open(outputfilename, "wb"))

result = get_sru_records(1, 1)
n = result[0]
match = re.search("\<zs:numberOfRecords\>(.*?)\</zs:numberOfRecords\>", n)
if match:
    all_records = match.group(1)
else:
    sys.exit("could get numberOfRecords")

all_records = int(all_records)
max_records_request = 100
start = 1

while start < all_records:

    oldrecords = get_sru_records(start, max_records_request)

    if not oldrecords:
        sys.exit("Anfrage nicht erfolgreich")

    if len(oldrecords) == 0:
        sys.exit("leere Liste")

    for oldrecord in oldrecords[:-1]:

        match = re.search("(\<record\>.*?\</record\>)", oldrecord)

        if not match:
            sys.exit("kein record-Element")

        oldrecord = match.group(1)
        oldrecord = oldrecord.encode("utf-8")
        oldrecord = BytesIO(oldrecord)
        oldrecord = pymarc.marcxml.parse_xml_to_array(oldrecord)
        oldrecord = oldrecord[0]

        newrecord = marcx.Record.from_record(oldrecord)
        newrecord.force_utf8 = True
        newrecord.strict = False

        # Prüfen, ob ein Titel vorhanden ist
        try:
            newrecord["245"]["a"]
        except:
            continue

        # Prüfen, ob es sich um Digitalisat handelt bzw. ein Link zu einem Digitalisat enthalten ist
        f856 = newrecord["856"]
        if not f856 or not has_digitalization_links(newrecord):
            continue

        # Formatfestlegung, pauschal für gesamte Quelle
        format = "Score"

        # Leader
        newrecord.remove_fields("Leader")
        leader = formats[format]["Leader"]
        newrecord.leader = leader

        # Identifikator
        f001 = newrecord["001"].data
        newrecord.remove_fields("001")
        newrecord.add("001", data="finc-14-" + f001)

        # Zugangsfacette
        f007 = formats[format]["e007"]
        newrecord.add("007", data=f007)

        # RDA-Inhaltstyp
        f336b = formats[format]["336b"]
        newrecord.add("336", b=f336b)

        # RDA-Datenträgertyp
        f338b = formats[format]["338b"]
        newrecord.add("338", b=f338b)

        # Link zum Digitalisat
        digitalization_links = list(get_digitalization_links(newrecord))
        newrecord.remove_fields("856")
        if len(digitalization_links) > 0:
            for digitalization_link in digitalization_links:
                newrecord.add("856", q="text/html", _3="Link zum Digitalisat", u=digitalization_link)

        # Link zum Datensatz
        id = re.sub("^00000", "", f001)
        newrecord.add("856", q="text/html", _3="Link zum Datensatz", u="https://opac.rism.info/search?id=" + id)

        # SWB-Inhaltstyp
        f935c = formats[format]["935c"]
        newrecord.add("935", c=f935c)

        # 970
        newrecord.add("970", c="PN")

        # Ansigelung und Kollektion
        collections = ["a", f001, "b", "14", "c", "sid-14-col-rism"]
        newrecord.add("980", subfields=collections)

        marc_clean_record(newrecord)
        writer.write(newrecord)

    start += max_records_request

writer.close()
