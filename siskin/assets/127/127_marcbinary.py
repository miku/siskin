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

Source: Filmuniversität Babelsberg Konrad Wolf (VK Film)
SID: 127
Ticket: #8575, #13016, #14547, #14898

"""

import io
import re
import sys

import xmltodict

import marcx
from siskin.mab import MabXMLFile
from siskin.mappings import formats
from siskin.utils import (check_isbn, check_issn, convert_to_finc_id, marc_build_field_008)


def get_valid_language(record, field):
    """
    Takes languages codes and returns the first in a valid form.
    """
    languages = record.field(field, alt="")
    if "$$" in languages:
        languages = languages.split("$$")
    elif ";" in languages:
        languages = languages.split(" ; ")
    else:
        languages = languages.split("$")
    return languages[0]


def get_clean_url(record, url):
    """
    Takes a URL and returns it without subfield codes and other fragments.
    """
    match1 = re.search("^(http.*?)\$", url)
    match2 = re.search("\$(.*?)\$", url)
    if match1:
        url = match1.group(1)
    elif match2:
        url = match2.group(1)
    return url


def get_clean_subject(subject):
    """
    Takes a subject and returns it without old subfield codes and other invalid chars.
    """
    subject = subject.replace("/C/", " ; ")
    match1 = re.search("(.*?)\/\w\$", subject)
    match2 = re.search("(.*?)\$", subject)
    if match1:
        subject = match1.group(1)
    elif match2:
        subject = match2.group(1)
    return subject


inputfilename = "127_input.xml"
outputfilename = "127_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

reader = MabXMLFile(inputfilename, replace=(u"�", ""), encoding='utf-8')
outputfile = open(outputfilename, "wb")

parent_ids = []
parent_titles = {}

for record in reader:

    id = record.field("010", alt="")
    if id:
        parent_ids.append(id)

for record in reader:

    id = record.field("001")
    if id in parent_ids:
        title = record.field("331")
        parent_titles[id] = title

for record in reader:

    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False

    # Formaterkennung
    url = record.field("655", alt="")
    if url and "http://d-nb.info" not in url:
        access = "electronic"
    else:
        access = "physical"

    f050 = record.field("050", alt="")
    f051 = record.field("051", alt="")
    f052 = record.field("052", alt="")
    f519 = record.field("519", alt="")
    f001 = record.field("001")
    match = re.search("^a", f051)  # Artikel

    # a||||||||||||| --> Buch
    # |||||ce|d||||| --> Video auf optischem Speichermedium
    # |||||ce||||||| --> Video
    # |||||ca||||||| --> Videobandkassette
    # |||||aa||||||| --> Audio-CD
    # |||||ba||||||| --> Filmrolle
    # |||||aj||||||| --> Schallplatte
    # a|a||||||||||| --> Buch
    # ||||||||d||||| --> CD-ROM
    # |||||ba|d||||| --> Filmspule auf optischem Speichermedium?
    # ||||||||g||||| --> Computerdatei im Fernzugriff
    # a|a|||||z||||| --> Computerdatei
    # a|||||||g||||| --> Computerdatei im Fernzugriff
    # |||||ad||||||| --> Kompaktkassette
    # a|a|||||g||||| --> Computerdatei im Fernzugriff
    # ||||||||z||||| --> Computerdatei
    # |||||ac||||||| --> Tonband
    # ||||||||e||||| --> Einsteckmodul
    # |||||dc||||||| --> Videodisc
    # ||||||||a||||| --> Computerdatei
    # a|||||||d||||| --> optisches Speichermedium
    # ||a|||||d||||| --> optisches Speichermedium
    # |||||aa|d||||| --> Audio-CD
    # |||||ba| ||||| --> Filmrolle

    if f001 in parent_ids:
        format = "Multipart"
    elif f519:
        format = "Thesis"
    elif f052:
        format = "Journal"
    elif match:
        format = "Article"
    elif f050 == "a|||||||||||||":
        format = "Book"
    elif f050 == "|||||ce|d|||||":
        format = "CD-Video"
    elif f050 == "|||||ce|||||||":
        format = "CD-Video"
    elif f050 == "|||||ca|||||||":
        format = "Video-Cassette"
    elif f050 == "|||||aa|||||||":
        format = "CD-Audio"
    elif f050 == "|||||ba|||||||":
        format = "Film-Role"
    elif f050 == "|||||aj|||||||":
        format = "Vinyl-Record"
    elif f050 == "a|a|||||||||||":
        format = "Book"
    elif f050 == "||||||||d|||||":
        format = "CD-ROM"
    elif f050 == "|||||ba|d|||||":
        format = "Film-Role"
    elif f050 == "||||||||g|||||":
        format = "Remote-Computerfile"
    elif f050 == "a|a|||||z|||||":
        format = "Local-Computerfile"
    elif f050 == "a|||||||g|||||":
        format = "Remote-Computerfile"
    elif f050 == "|||||ad|||||||":
        format = "Audio-Cassette"
    elif f050 == "a|a|||||g|||||":
        format = "Remote-Computerfile"
    elif f050 == "||||||||z|||||":
        format = "Local-Computerfile"
    elif f050 == "|||||ac|||||||":
        format = "Audio-Cassette"
    elif f050 == "||||||||e|||||":
        format = "Object"
    elif f050 == "|||||dc|||||||":
        format = "CD-Video"
    elif f050 == "||||||||a|||||":
        format = "Local-Computerfile"
    elif f050 == "a|||||||d|||||":
        format = "CD-ROM"
    elif f050 == "||a|||||d|||||":
        format = "CD-ROM"
    elif f050 == "|||||aa|d|||||":
        format = "CD-Audio"
    elif f050 == "|||||ba| |||||":
        format = "Film-Role"
    else:
        format = "Book"

    # Leader
    leader = formats[format]["Leader"]
    marcrecord.leader = leader

    # Identifikator
    f001 = record.field("001")
    f001 = "127-" + f001
    marcrecord.add("001", data=f001)

    # Zugangstyp
    if access == "physical":
        f007 = formats[format]["p007"]
    else:
        f007 = formats[format]["e007"]
    marcrecord.add("007", data=f007)

    # Periodizität
    year = record.field("425", alt="")
    periodicity = formats[format]["008"]
    language = get_valid_language(record, "037")
    f008 = marc_build_field_008(year, periodicity, language)
    marcrecord.add("008", data=f008)

    # ISBN
    isbns = record.fields("540")
    for isbn in isbns:
        f020a = check_isbn(isbn)
        marcrecord.add("020", a=f020a)

    # ISSN
    f022a = ""
    issns = record.fields("542")
    for issn in issns:
        f022a = check_issn(issn)
        marcrecord.add("022", a=f022a)

    # Sprache
    f041a = get_valid_language(record, "037")
    marcrecord.add("041", a=f041a)

    # 1. Urheber
    f100a = record.field("100", alt="")
    if "$$" in f100a:
        f100a = f100a.split("$$")
        f100a = f100a[0]
    marcrecord.add("100", a=f100a)

    # Werktitel
    f240a = record.field("304")
    marcrecord.add("240", a=f240a)

    # Haupttitel, Titelzusatz und Verantwortliche
    f010 = record.field("010")
    if not f010:
        f245a = record.field("331", alt="")
        f245p = ""
    else:
        f245a = parent_titles[f010]
        f245p = record.field("331", alt="")
    f245b = record.field("335", alt="")
    f245c = record.field("359", alt="")
    f245n = record.field("089", alt="")
    marcrecord.add("245", a=f245a, b=f245b, c=f245c, n=f245n, p=f245p)

    # Parallel- und Alternativtitel
    f246a = record.field("340", alt="")
    marcrecord.add("246", a=f246a)
    f246a = record.field("370", alt="")
    marcrecord.add("246", a=f246a)

    # beigefügtes Werk
    f249a = record.field("361", alt="")
    marcrecord.add("249", a=f249a)

    # Ausgabe
    f250a = record.field("403", alt="")
    marcrecord.add("250", a=f250a)

    # Erscheinungsvermerk
    f260a = record.field("410", alt="")
    f260b = record.field("412", alt="")
    f260c = record.field("425", alt="")
    marcrecord.add("260", a=f260a, b=f260b, c=f260c)

    # Umfangsangabe
    f300a = record.field("433", alt="")
    f300b = record.field("434", alt="")
    f300c = record.field("435", alt="")
    marcrecord.add("300", a=f300a, b=f300b, c=f300c)

    # RDA-Inhaltstyp
    f336b = formats[format]["336b"]
    marcrecord.add("336", b=f336b)

    # RDA-Datenträgertyp
    f338b = formats[format]["338b"]
    marcrecord.add("338", b=f338b)

    # Fußnote
    f500a = record.field("501", alt="")
    if "FSK" not in f500a:
        marcrecord.add("500", a=f500a)

    # Hochschulvermerk
    f502a = record.field("519", alt="")
    marcrecord.add("502", a=f502a)
    f502a = record.field("520", alt="")
    marcrecord.add("502", a=f502a)

    # Inhaltszusammenfassung
    fields = ["750", "753", "754", "755", "756", "757", "758"]
    for field in fields:
        f520a = record.field(field, alt="")
        marcrecord.add("520", a=f520a)

    subfields = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m"]
    for subfield in subfields:
        f520a = record.field("750", subfield, alt="")
        marcrecord.add("520", a=f520a)

    # Systemvoraussetzung
    f538a = record.field("651", alt="")
    marcrecord.add("538", a=f538a)

    # FSK-Hinweis
    f521a = record.field("501", alt="")
    if "FSK" in f500a:
        marcrecord.add("521", a=f521a)

    # Schlagwörter
    all_subjects = set()
    subjects = record.fields("710")
    if subjects:
        for subject in subjects:
            subject = get_clean_subject(subject)
            all_subjects.add(subject)

    subjects = record.field("720", alt="")
    if " / " in subjects:
        subjects = subjects.split(" / ")
    else:
        subjects = subjects.split(" ; ")

    for subject in subjects:
        subject = get_clean_subject(subject)
        all_subjects.add(subject)

    for subject in all_subjects:
        marcrecord.add("650", a=subject)

    # GND-Inhalts- und Datenträgertyp
    f655a = formats[format]["655a"]
    f6552 = formats[format]["6552"]
    marcrecord.add("655", a=f655a, _2=f6552)

    # weitere Urheber
    fields = [
        "100", "104", "108", "112", "116", "120", "124", "128", "132", "136", "140", "144", "148", "152", "156", "160", "164", "168", "172", "176", "180",
        "184", "188", "192", "196"
    ]

    for field in fields:
        f700a = record.field(field, alt="")
        if "$$" in f700a:
            f700a = f700a.split("$$")
            f700a = f700a[0]
        if f700a != f100a:
            marcrecord.add("700", a=f700a)

    # übergeordnetes Werk
    f773g = record.field("596", alt="")
    match = re.search("(.*)\. - Sign", f773g)
    if match:
        f773g = match.group(1)
    year = record.field("425", alt="")
    match = re.match("\d\d\d\d", year)
    if f773g and match:
        f773g = year + ". - " + f773g
    f773t = record.field("590", alt="")
    if not f773t:
        f773t = record.field("597", alt="")
    f773w = record.field("010", alt="")
    marcrecord.add("773", t=f773t, g=f773g, w=f773w, x=f022a)

    # Link zu Ressource oder Inhaltsverzeichnis
    # beginnt teilweise mit htm$$u und endet teilweise mit $$
    # http://www.shortfilm.ch
    # http://www.ses.fi$$9htm
    # htm$$uhttp://www.freidok.uni-freiburg.de/volltexte/127/$$

    # Volltexte
    # "http://www.bibliothek.uni-regensburg.de/ezeit/" in URL
    # "Volltext" in URL
    # "http://www.oapen.org/search?identifier" in URL
    # "http://dx.doi.org/" in URL

    url = record.field("655", alt="")
    if url:

        if "Volltext" in url or "http://www.bibliothek.uni-regensburg.de/ezeit/" in url or "http://www.oapen.org/search?identifier" in url or "http://dx.doi.org/" in url:
            f8563 = "Link zur Ressource"
            f856u = get_clean_url(record, url)
            marcrecord.add("856", _3=f8563, u=f856u)

        if "Inhalt" in url:
            f8563 = "Link zum Inhaltsverzeichnis"
            f856u = get_clean_url(record, url)
            marcrecord.add("856", _3=f8563, u=f856u)

    # Link zum Datensatz
    id = record.field("001")
    marcrecord.add("856",
                   _3="Link zu Filmuniversität Babelsberg Konrad Wolf",
                   u="http://server8.bibl.filmuniversitaet.de/F/?func=find-c&ccl_term=idn=%s&local_base=HFF01" % id)

    # Kollektion
    marcrecord.add("912", a="vkfilm")

    # SWB-Inhaltstyp
    f935c = formats[format]["935c"]
    marcrecord.add("935", c=f935c)

    # Ansigelung
    f001 = record.field("001")
    collections = ["a", f001, "b", "127", "c", "sid-127-col-filmunivpotsdam"]
    marcrecord.add("980", subfields=collections)

    marcrecord = convert_to_finc_id("127", marcrecord, encode=False, finc_prefix=True)

    outputfile.write(marcrecord.as_marc())

outputfile.close()
