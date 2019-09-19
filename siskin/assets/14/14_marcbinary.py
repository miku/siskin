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

Source: RISM
SID: 14
Ticket: 1145, #4435, #16018

"""

import io
import sys
from builtins import *

import marcx
import pymarc
from siskin.mappings import formats
from siskin.utils import marc_clean_record


copytags = ("003", "004", "005", "006", "008", "009", "010", "011", "012", "013", "014", "015", "016", "017", "018",
            "019", "020", "021", "022", "023", "024", "025", "026", "027", "028", "029", "030", "031", "032", "033",
            "034", "035", "036", "037", "038", "039", "040", "041", "042", "043", "044", "045", "046", "047", "048",
            "049", "050", "051", "052", "053", "054", "055", "056", "057", "058", "059", "060", "061", "062", "063",
            "064", "065", "066", "067", "068", "069", "070", "071", "072", "073", "074", "075", "076", "077", "078",
            "079", "080", "081", "082", "083", "085", "086", "087", "088", "089", "090", "091", "092", "093", "094",
            "095", "096", "097", "098", "099", "100", "101", "102", "103", "104", "105", "106", "107", "108", "110",
            "111", "112", "113", "114", "115", "116", "117", "118", "120", "121", "124", "125", "126", "129", "130",
            "131", "134", "135", "138", "142", "145", "148", "149", "151", "155", "156", "157", "160", "162", "163",
            "164", "165", "166", "167", "173", "174", "175", "176", "178", "183", "184", "187", "192", "200", "204",
            "210", "212", "234", "240", "243", "254", "256", "260", "300", "340", "383", "386", "387", "447", "448",
            "456", "490", "500", "504", "510", "518", "520", "541", "561", "563", "590", "591", "592", "593", "594",
            "595", "596", "597", "650", "657", "700", "710", "730", "762", "773", "775", "787")


def has_digitalization_links(record):
    for field_856 in record.get_fields("856"):
        for x in field_856.get_subfields("x"):
            if "Digitalization" == x:
                return True
        for z in field_856.get_subfields("z"):
            if "digit" in z or "Digit" in z and not ("Bach digital" in z or "Bach Digital" in z):
                return True
            if "exemplar" in z or "Exemplar" in z:
                return True

def get_digitalization_links(record):
    """
    Given a marc record, just return all links (from 856.u).

    Same as:

        >>> record = marcx.Record()
        >>> record.add('856', u='http://google.com')
        >>> list(record.itervalues('856.u'))
        ['http://google.com']

    """
    for field_856 in record.get_fields("856"):
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


def get_titles(record):
    for field in record.get_fields("772"):
        for title in field.get_subfields("a"):
            yield title


def get_field(record, field, subfield="a"):
    """
    Shortcut to get a record field and subfield or empty string, if no value is found.
    """
    try:
        value = record[field][subfield]
        return value if value is not None else ""
    except (KeyError, TypeError):
        return ""


# Default input and output.
inputfilename, outputfilename = "14_input.mrc", "14_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = io.open(inputfilename, "rb")
outputfile = io.open(outputfilename, "wb")
reader = pymarc.MARCReader(inputfile)

format = "Score"

for oldrecord in reader:

    newrecord = marcx.Record()
    newrecord.force_utf8 = True
    newrecord.strict = False

    # prüfen, ob Titel vorhanden ist
    f245 = oldrecord["245"]
    if not f245:
        continue

    # prüfen, ob es sich um Digitalisat handelt bzw. ein Link zu einem Digitalisat enthalten ist
    f856 = oldrecord["856"]
    if not f856 or not has_digitalization_links(oldrecord):
        continue

    for field in oldrecord.get_fields("856"):
        f856 = field if "http" in field else ""

    # leader
    leader = formats[format]["Leader"]
    newrecord.leader = leader

    # 001
    f001 = oldrecord["001"].data
    newrecord.add("001", data="finc-14-%s" % f001)

    # Zugangsfacette
    f007 = formats[format]["e007"]
    newrecord.add("007", data=f007)

    # Originalfelder, die ohne Änderung übernommen werden
    for tag in copytags:
        for field in oldrecord.get_fields(tag):
            newrecord.add_field(field)

    # Haupttitel
    f240a = get_field(oldrecord, "240", "a")
    f240m = get_field(oldrecord, "240", "m")
    f240n = get_field(oldrecord, "240", "n")
    f245a = "%s %s %s" % (f240a, f240m, f240n)
    newrecord.add("245", a=f245a)

    # Alternativtitel
    f246a = get_field(oldrecord, "245", "a")
    if f246a != "[without title]":
        newrecord.add("246", a=f246a)

    # RDA-Inhaltstyp
    f336b = formats[format]["336b"]
    newrecord.add("336", b=f336b)

    # RDA-Datenträgertyp
    f338b = formats[format]["338b"]
    newrecord.add("338", b=f338b)

    # Fußnote
    f852a = get_field(oldrecord, "852", "a")
    f852c = get_field(oldrecord, "852", "c")  # häufig None
    f852x = get_field(oldrecord, "852", "x")
    f500a = "%s, %s, %s" % (f852a, f852c, f852x)
    newrecord.add("500", a=f500a)

    # enthaltene Werke
    try:
        titles = list(get_titles(oldrecord))
        if titles:
            for title in titles:
                newrecord.add("505", a=title)
    except (KeyError, TypeError):
        pass

    # 856 (Digitalisat)
    digitalization_links = list(get_digitalization_links(oldrecord))
    if len(digitalization_links) > 0:
        for digitalization_link in digitalization_links:
            newrecord.add("856", q="text/html", _3="Link zum Digitalisat", u=digitalization_link)

    # 856 (Datensatz)
    newrecord.add("856", q="text/html", _3="Link zum Datensatz", u="https://opac.rism.info/search?id=" + f001)

    # SWB-Inhaltstyp
    f935c = formats[format]["935c"]
    newrecord.add("935", c=f935c)

    # 970
    newrecord.add("970", c="PN")

    # Ansigelung
    newrecord.add("980", a=f001, b="14", c="sid-14-col-rism")

    marc_clean_record(newrecord)
    outputfile.write(newrecord.as_marc())

inputfile.close()
outputfile.close()
