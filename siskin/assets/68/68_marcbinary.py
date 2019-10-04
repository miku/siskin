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

Source: Online Contents (OLC)
SID: 68
Ticket: #5163, #6743, #9354, #10294 

"""

import io
import json
import os
import re
import sys

from siskin.mappings import formats
from siskin.utils import check_isbn, check_issn, marc_build_field_008


def remove_field(record, field):
    try:
        del record[field]
    except:
        pass


inputfilename = "68_input.json"
outputfilename = "68_output.json"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = open(inputfilename, "r")
outputfile = open(outputfilename, "w")

for line in inputfile:

    record = json.loads(line)
    fullrecord = record["fullrecord"]
    
    if "SSG-OLC-FTH" in fullrecord or "SSG-OLC-MKW" in fullrecord:

        id = record["id"]
        id = id.replace("dswarm-", "")
        record["id"] = id
      
        remove_field(record, "vf1_author")
        remove_field(record, "vf1_author_role")
        remove_field(record, "vf1_author_orig")
        remove_field(record, "vf1_author2")
        remove_field(record, "vf1_author2_role")
        remove_field(record, "vf1_author2-role")
        remove_field(record, "vf1_author2_orig")
        remove_field(record, "vf1_author_corp")
        remove_field(record, "vf1_author_corp_orig")
        remove_field(record, "vf1_author_corp2")
        remove_field(record, "vf1_author_corp2_orig")
        remove_field(record, "title_part")

        record = json.dumps(record)
        record = record + "\n"
        outputfile.write(str(record))
  
inputfile.close()
outputfile.close()
