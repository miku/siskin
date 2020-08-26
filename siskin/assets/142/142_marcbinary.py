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

Source: Gesamtkatalog der Düsseldorfer Kulturinstitute (VK Film)
SID: 142
Ticket: #8392

"""

import io
import re
import sys

import xmltodict

import marcx
from siskin.mab import MabXMLFile
from siskin.mappings import formats
from siskin.utils import (check_isbn, check_issn, convert_to_finc_id, marc_build_field_008, marc_get_languages)

inputfilename = "142_input.xml"
outputfilename = "142_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

reader = MabXMLFile(inputfilename, replace=(u"‰", ""), encoding='utf-8')
outputfile = open(outputfilename, "wb")

series_ids = []
parent_ids = []
parent_titles = {}

for record in reader:

    parent_id = record.field("010", alt="")
    if len(parent_id) > 0:
        parent_ids.append(parent_id)

    series_id = record.field("453", alt="")
    if len(series_id) > 0:
        series_ids.append(series_id)

    series_id = record.field("463", alt="")
    if len(series_id) > 0:
        series_ids.append(series_id)

for record in reader:

    id = record.field("001")
    title = record.field("331")

    if id in parent_ids:
        parent_titles[id] = title

for record in reader:

    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False

    parent_id = record.field("010", alt="")
    id = record.field("001")
    title = record.field("331")

    f245a = title
    f245p = ""
    f773w = ""

    if len(parent_id) > 0:
        has_parent_title = parent_titles.get(parent_id, None)
        if has_parent_title:
            f245a = parent_titles[parent_id]
            f245p = title
            f773w = "(DE-576)" + parent_id

    if not f245a:
        continue

    # Format
    format = record.field("433", alt="")
    regexp1 = re.search("\d\.?\sS", format)

    if id in parent_ids:
        format = "Multipart"
    elif id in series_ids:
        format = "Series"
    elif "Seiten" in format or "Blatt" in format or "nicht gez" in format or format == "" or u"Zählung" in format or regexp1:
        format = "Book"
    elif "Loseblatt" in format:
        format = "Loose-leaf"
    elif "Faltbl" in format or "Leporello" in format or "Postkarte" in format:
        format = "Object"
    elif "DVD-Video" in format or "1 DVD" in format or "DVDs" in format:
        format = "DVD-Video"
    elif "Audio-DVD" in format:
        format = "DVD-Audio"
    elif "Blu-ray" in format or "Blu-Ray" in format or "Bluray" in format:
        format = "Blu-Ray-Disc"
    elif "Videokassette" in format or "Videokasette" in format or "Video-Kassette" in format or "VHS" in format:
        format = "Video-Cassette"
    elif "Audiokassette" in format or " MC" in format or "1 Kassette" in format:
        format = "Audio-Cassette"
    elif "-ROM" in format or "-Rom" in format:
        format = "CD-ROM"
    elif "DVD-ROM" in format:
        format = "DVD-ROM"
    elif "Mikrofilm" in format or "Mikrofiche" in format or "Microfiche" in format:
        format = "Microform"
    elif "etr. gez." in format:
        format = "Newspaper"
    elif "Karte" in format:
        format = "Map"
    elif "CD " in format or " CD" in format or "-CD" in format or "Compact" in format or "Compakt" in format or "Cds" in format:
        format = "CD-Audio"
    elif "Diskette" in format:
        format = "Floppy-Disk"
    else:
        format = "Unknown"

    # Leader
    leader = formats[format]["Leader"]
    marcrecord.leader = leader

    # Identifikator
    f001 = record.field("001")
    f001 = "finc-142-" + f001
    marcrecord.add("001", data=f001)

    # Zugangsart
    f007 = formats[format]["p007"]
    marcrecord.add("007", data=f007)

    # Periodizität
    year = record.field("425", alt="")
    periodicity = formats[format]["008"]
    language = record.field("037", alt="")
    language = marc_get_languages(language)
    f008 = marc_build_field_008(year, periodicity, language)
    marcrecord.add("008", data=f008)

    # ISBN
    isbns = record.fields("540")
    for isbn in isbns:
        f020a = check_isbn(isbn)
        marcrecord.add("020", a=f020a)

    # ISSN
    issns = record.fields("542")
    for issn in issns:
        f022a = check_issn(issn)
        marcrecord.add("022", a=f022a)

    # Sprache
    f041a = record.field("037")
    if f041a:
        marcrecord.add("041", a=f041a)

    # 1. Schöpfer
    f100a = record.field("100")
    marcrecord.add("100", a=f100a)

    # 1. Körperschaft
    f110a = record.field("200")
    marcrecord.add("110", a=f110a)

    # Haupttitel & Verantwortlichenangabe
    f245b = record.field("335")
    f245c = record.field("359")
    f245 = ["a", f245a, "b", f245b, "c", f245c, "p", f245p]
    marcrecord.add("245", subfields=f245)

    # Erscheinungsvermerk
    f260a = record.field("410", alt="")
    f260b = record.field("412", alt="")
    f260c = record.field("425", alt="")
    subfields = ["a", f260a, "b", f260b, "c", f260c]
    marcrecord.add("260", subfields=subfields)

    # Umfangsangabe
    f300a = record.field("433")
    f300b = record.field("434")
    subfields = ["a", f300a, "b", f300b]
    marcrecord.add("300", subfields=subfields)

    # RDA-Inhaltstyp
    f336b = formats[format]["336b"]
    marcrecord.add("336", b=f336b)

    # RDA-Datenträgertyp
    f338b = formats[format]["338b"]
    marcrecord.add("338", b=f338b)

    # Reihe
    f490a = record.field("451")
    marcrecord.add("490", a=f490a)
    f490a = record.field("461")
    marcrecord.add("490", a=f490a)

    # GND-Inhalts- und Datenträgertyp
    f655a = formats[format]["655a"]
    f6552 = formats[format]["6552"]
    marcrecord.add("338", a=f655a, _2=f6552)

    # weitere geistige Schöpfer
    for i in range(104, 199, 4):
        tag = str(i)
        f700a = record.field(tag)
        marcrecord.add("700", a=f700a)

    # weitere Körperschaften
    for i in range(204, 299, 4):
        tag = str(i)
        f710a = record.field(tag)
        marcrecord.add("710", a=f710a)

    # übergeordnetes Werk
    marcrecord.add("773", w=f773w)

    # Link auf Reihe
    f830a = record.field("451")
    f830w = record.field("453", alt="")
    marcrecord.add("830", a=f830a, w=f830w)
    f830a = record.field("461")
    f830w = record.field("463", alt="")
    marcrecord.add("830", a=f830a, w=f830w)

    # Link zu Datensatz und Ressource
    f655z = record.field("655", "z")
    f6553 = record.field("655", "3")
    f856u = record.field("655", "u")
    if f655z:
        f856q = f655z
    else:
        f856q = f6553
    marcrecord.add("856", q=f856q, u=f856u)

    # Kollektion
    marcrecord.add("912", a="vkfilm")

    # SWB-Inhaltstyp
    f935c = formats[format]["935c"]
    marcrecord.add("935", c=f935c)

    # Ansigelung
    f001 = record.field("001")
    collections = ["a", f001, "b", "142", "c", "sid-142-col-gesamtkatduesseldorf"]
    marcrecord.add("980", subfields=collections)

    marcrecord = convert_to_finc_id("142", marcrecord, encode=False, finc_prefix=True)

    outputfile.write(marcrecord.as_marc())

outputfile.close()
