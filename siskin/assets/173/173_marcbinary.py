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

Source: Wolfenbütteler Bibliographie zur Geschichte des Buchwesens
SID: 173
Ticket: #13640

"""

import re
import sys

import marcx
import pymarc
from siskin.utils import marc_clean_record


def get_field(record, field, subfield):
    try:
        return record[field][subfield]
    except:
        return ""


def get_inds_for_field_245(title):
    """
    Checks if the title starts with an article and returns the appropriate.
    """
    
    if title.startswith("A "):
        return " 2"
    elif title.startswith("Un "):
        return " 3"
    elif title.startswith("Ein "):
        return " 4"
    elif title.startswith("Der "):
        return " 4"
    elif title.startswith("Die "):
        return " 4"
    elif title.startswith("Das "):
        return " 4"
    elif title.startswith("The "):
        return " 4"
    elif title.startswith("Une "):
        return " 4"
    elif title.startswith("Una "):
        return " 4"
    elif title.startswith("Eine "):
        return " 5"
    else:
        return "  "


inputfilename = "173_input.mrc"
outputfilename = "173_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = open(inputfilename, "rb")
outputfile = open(outputfilename, "wb")

reader = pymarc.MARCReader(inputfile, force_utf8=True)

for record in reader:

    record = marcx.Record.from_record(record)
    record.force_utf8 = True
    record.strict = False

    try:
        parent_type = record["773"]["7"]
    except:
        parent_type = ""

    # Leader
    if parent_type == "nnam":
        f935b = "druck"
        f935c = ""
        f008 = ""
        leader = "     " + record.leader[5:]
        record.leader = leader
    elif parent_type == "nnas":
        f935b = "druck"
        f935c = "text"
        f008 = "                     p"
        leader = record.leader[8:]
        leader = "     nab" + leader
        record.leader = leader
    else:
        f935b = "druck"
        f935c = ""
        f008 = ""
        leader = "     " + record.leader[5:]
        record.leader = leader

    # Identifikator
    f001 = record["001"].data
    regexp = re.search("edoc/(.*)", f001)
    if regexp:
        f001 = regexp.group(1)
    else:
        sys.exit("Die ID konnte nicht verarbeitet werden: " + f001)
    f001 = f001.replace(":", "")
    record.remove_fields("001")
    record.add("001", data="finc-173-%s" % f001)

    # Zugangsformat
    record.add("007", data="tu")

    # Periodizität
    record.add("008", data=f008)

    # Titel
    f245a = get_field(record, "245", "a")
    if f245a:
        record.remove_fields("245")
        f245a = re.sub("\s?~?\\\dagger\s?", "", f245a)
        inds = get_inds_for_field_245(f245a)
        record.add("245", a=f245a, indicators=inds)
    else:
        continue

    # Anmerkung
    f553a = get_field(record, "553", "a")
    if f553a:
        f553a = re.sub("\s?~?\\\dagger", "", f553a)
        record.remove_fields("553")
        record.add("553", a=f553a)

    # SWB-Format
    record.add("935", b=f935b, c=f935c)

    # Ansigelung und Kollektion
    collections = ["a", f001, "b", "173", "c", "sid-173-col-buchwesen"]
    record.add("980", subfields=collections)

    marc_clean_record(record)
    outputfile.write(record.as_marc())

inputfile.close()
outputfile.close()
