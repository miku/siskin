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

Source: Filmakademie Baden-Württemberg (VK Film)
SID: 151
Ticket: #8700, #15871, #16012

"""

import io
import re
import sys

import marcx

from siskin.mappings import formats, roles
from siskin.utils import check_isbn, marc_build_field_008
from six.moves import html_parser


parser = html_parser.HTMLParser()


def get_field(field, tag):
    regexp = re.search('<.*? tag="%s">(.*)$' % tag, field)
    if regexp:
        _field = regexp.group(1)
        _field = parser.unescape(_field)
        return _field
    else:
        return ""


def get_subfield(field, tag, subfield):
    regexp = re.search('^<datafield.*tag="%s".*><subfield code="%s">(.*?)<\/subfield>' % (tag, subfield), field)
    if regexp:
        _field = regexp.group(1)
        _field = parser.unescape(_field)
        return _field
    else:
        return ""


# default input and output
inputfilename = "151_input.xml"
outputfilename = "151_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = io.open(inputfilename, "r")
outputfile = io.open(outputfilename, "wb")

records = inputfile.read()
records = records.split("</record>")

for record in records:

    format = ""
    form = ""
    f001 = ""
    f020a = ""
    f100a = ""
    f100e = ""
    f245a = ""
    f260a = ""
    f260b = ""
    f260c = ""
    f300a = ""
    f300b = ""
    f300c = ""
    f650a = ""
    f700a = ""
    f700e = ""

    subjects = []
    persons = []
    f100 = []
    f700 = []

    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False
    fields = re.split("(</controlfield>|</datafield>)", record)

    for field in fields:

        field = field.replace("\n", "")
        field = field.replace("</datafield>", "")
        field = field.replace("</controlfield>", "")

        form = get_field(field, "433")

        # format recognition
        if form and not format:
            
            regexp1 = re.search("\d\]?\sS\.", form)
            regexp2 = re.search("\d\]?\sSeit", form)
            regexp3 = re.search("\d\]?\sBl", form)
            regexp4 = re.search("\s?Illl?\.", form)
            regexp5 = re.search("[XVI],\s", form)
            regexp6 = re.search("^\d+\s[SsPp]", form)
            regexp7 = re.search("^DVD", form)
            regexp8 = re.search("^Blu.?-[Rr]ay", form)
            regexp9 = re.search("^H[DC] [Cc][Aa][Mm]", form)
            regexp10 = re.search("^HDCAM", form)
            regexp11 = re.search("[Bb]et.?-?[Cc]am", form)
            regexp12 = re.search("CD", form)
            regexp13 = re.search("[kKCc]asss?ette", form)
            regexp14 = re.search("^VHS", form)
            regexp15 = re.search("^Noten", form)
            regexp16 = re.search("^Losebl", form)
            regexp17 = re.search("^Film\s?\[", form)
            regexp18 = re.search("\d\smin", form)
            regexp19 = re.search("S\.\s\d+\s?-\s?\d+", form)

            if regexp1 or regexp2 or regexp3 or regexp4 or regexp5 or regexp6:
                format = "Book"
            elif regexp7:
                format = "DVD-Video"
            elif regexp8:
                format = "Blu-Ray-Disc"
            elif regexp9 or regexp10 or regexp11:
                format = "CD-Video"
            elif regexp12:
                format = "CD-Audio"
            elif regexp13 or regexp14:
                format = "Video-Cassette"
            elif regexp15:
                format = "Score"
            elif regexp16:
                format = "Loose-leaf"
            elif regexp17 or regexp18:
                format = "CD-Video"
            elif regexp19:
                format = "Article"                

        if f001 == "":
            f001 = get_field(field, "001")            

        if f020a == "":
            f020a = get_subfield(field, "540", "a")
        
        if f100a == "":
            f100a = get_subfield(field, "100", "a")
            if f100a != "":
                f100.append("a")
                f100.append(f100a)
                role = get_subfield(field, "100", "b")
                if role != "":
                    match = re.search("\[(.*?)\]", role)
                    if match:
                        role = match.group(1)
                        role = role.lower()
                        role = role.replace(".", "")
                        f1004 = roles.get(role, "")
                        if not f1004:
                            print("Missing role: %s." % role)
                        f100.append("4")
                        f100.append(f1004)
       
        if f245a == "":
            f245a = get_field(field, "331")
         
        if f260a == "":
            f260a = get_field(field, "410")

        if f260b == "":
            f260b = get_field(field, "412")

        if f260c == "":
            f260c = get_field(field, "425")

        if f300a == "":
            f300 = get_field(field, "433")
            # 335 S. : zahlr. Ill. ; 32 cm
            regexp1 = re.search("(.*)\s?:\s(.*);\s(.*)", f300)
            # 289 S.: Zahlr. Ill.
            regexp2 = re.search("(.*)\s?:\s(.*)", f300)
            # 106 S. ; 21 cm
            regexp3 = re.search("(.*)\s?;\s(.*)", f300)

            if regexp1:
                f300a, f300b, f300c = regexp1.groups()
            elif regexp2:
                f300a, f300b = regexp2.groups()
            elif regexp3:
                f300a, f300c = regexp3.groups()
            else:
                f300a = f300

        f650a = get_subfield(field, "710", "a")
        if f650a != "":
            subjects.append(f650a)

        regexp = re.search('tag="1\d\d"', field) # checks if there is a person field
        if regexp:
            for i in range(101, 197):  
                f700a = get_subfield(field, i, "a")
                if f700a != "":
                    f700.append("a")
                    f700.append(f700a)
                    role = get_subfield(field, i, "b")
                    if role != "":
                        match = re.search("\[(.*?)\]", role)
                        if match:
                            role = match.group(1)
                            role = role.lower()
                            role = role.replace(".", "")
                            f7004 = roles.get(role, "")
                            if not f7004:
                                print("Missing role: %s." % role)
                            f700.append("4")
                            f700.append(f7004)
                    persons.append(f700)
                    f700 = []
                    break

    if not f001:
        continue

    if not format:
        format = "Book"
        # print("Missing format: %s (Default = Book)" % form)

    # leader
    leader = formats[format]["Leader"]     
    marcrecord.leader = leader

    # identifier
    marcrecord.add("001", data="finc-151-" + f001)
    
    # access
    f007 = formats[format]["p007"]
    marcrecord.add("007", data=f007)

    # year, periodicity, language
    year = f260c
    periodicity = formats[format]["008"]
    language = ""
    f008 = marc_build_field_008(year, periodicity, language)
    marcrecord.add("008", data=f008)
    
    # ISBN
    f020a = check_isbn(f020a)
    marcrecord.add("020", a=f020a)

    # 1. creator
    marcrecord.add("100", subfields=f100)

    # title
    marcrecord.add("245", a=f245a)
    
    # imprint
    publisher = ["a", f260a, "b", f260b, "c", f260c]
    marcrecord.add("260", subfields=publisher)

    # physical description
    physicaldescription = ["a", f300a, "b", f300b, "c", f300c]
    marcrecord.add("300", subfields=physicaldescription)

    # RDA content
    f336b = formats[format]["336b"]
    marcrecord.add("336", b=f336b)

    # RDA carrier
    f338b = formats[format]["338b"]
    marcrecord.add("338", b=f338b)

    # subjects
    for f650a in subjects:
        marcrecord.add("650", a=f650a)

    # addional creator
    for f700 in persons:
        marcrecord.add("700", subfields=f700)

    # collection
    marcrecord.add("912", a="vkfilm")

    # SWB content
    f935c = formats[format]["935c"]
    marcrecord.add("935", c=f935c)

    # Ansigelung 
    collections = ["a", f001, "b", "151", "c", "sid-151-col-filmakademiebawue"]
    marcrecord.add("980", subfields=collections)

    outputfile.write(marcrecord.as_marc())

inputfile.close()
outputfile.close()
