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

Source: Universitätsbibliothek Frankfurt am Main (VK Film)
SID: 119
Ticket: #8571, #14654, #15877

"""

import io
import re
import sys

import marcx
import pymarc
from siskin.utils import marc_clean_record


def check_fulltext(record):
    """
    Checks several fields and returns True, if there is probably a link to fulltext.
    """
    try:
        type = record["024"]["2"]
    except:
        type = ""

    try:
        urls = record.get_fields("856")
    except:
        urls = ""

    for url in urls:
        description = url.get_subfields("3")
        if not description or "kostenfrei" in description or "Digitalisierung" in description or "zugänglich" in description:
            has_fulltext_url = True
            break
    else:
        has_fulltext_url = False

    if (urls and has_fulltext_url) or (urls and (format == "doi" or format == "urn")):
        return True
    else:
        return False


inputfilename = "119_input.mrc"
outputfilename = "119_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = open(inputfilename, "rb")
outputfile = open(outputfilename, "wb")

reader = pymarc.MARCReader(inputfile)

for record in reader:

    record = marcx.Record.from_record(record)
    record.force_utf8 = True
    record.strict = False

    # Identifikator
    try:
        f001 = record["001"].data
    except AttributeError:
        continue
    record.remove_fields("001")
    record.add("001", data="finc-119-" + f001)

    # Zugangsfacette
    has_fulltext = check_fulltext(record)
    try:
        f007 = record["007"].data
    except:
        f007 = ""

    if f007 and not has_fulltext:
        if f007.startswith("cr"):
            record.remove_fields("007")
            record.add("007", data="tu")
    elif has_fulltext:
        record.remove_fields("007")
        record.add("007", data="cr")
    elif not f007:
        record.add("007", data="tu")

    # Kollektion
    record.remove_fields("912")
    record.add("912", a="vkfilm")

    # Ansigelung
    collections = ["a", f001, "b", "119", "c", "sid-119-col-ubfrankfurt"]
    record.add("980", subfields=collections)

    marc_clean_record(record)
    outputfile.write(record.as_marc())

outputfile.close()
