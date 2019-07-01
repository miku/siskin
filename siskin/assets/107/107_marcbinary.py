#!/usr/bin/env python3
# coding: utf-8

import io
import re
import sys

import xmltodict

import marcx
from siskin.mappings import formats
from siskin.utils import check_isbn, check_issn, marc_build_field_008


def get_field(name):

    multivalued_fields = ["dc:identifier", "dc:subject", "dc:language"]
    field = xmlrecord["metadata"]["oai_dc:dc"].get(name, None)
    if not field:
        return ""
    if isinstance(field, list) and name not in multivalued_fields:
        print("Das Feld %s liegt teilweise als Liste vor: " % name)
    return field


inputfilename = "107_input.xml"
outputfilename = "107_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

inputfile = open(inputfilename, "rb")
outputfile = open(outputfilename, "wb")

xmlfile = inputfile.read()
xmlrecords = xmltodict.parse(xmlfile, force_list="dc:identifier")

for xmlrecord in xmlrecords["Records"]["Record"]:

    if not get_field("dc:title"):
        continue

    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False

    # Formatzuordnung
    #Article</dc:type>
    #Charter</dc:type>
    #Fragment</dc:type>
    #Letter</dc:type>
    #Manuscript</dc:type>
    #Map</dc:type>
    #Monograph</dc:type>
    #Multivolume_work</dc:
    #Periodical</dc:type>
    #Series</dc:type>
    #Text</dc:type>
    #Volume</dc:type>
    format = get_field("dc:type")

    if format == "Article":
        format = "Article"
    elif format == "Manuscript":
        format = "Manuscript"
    elif format == "Periodical":
        format = "Journal"
    elif format == "Multivolume_work":
        format = "Multipart"
    elif format == "Map":
        format = "Map"
    else:
        format = "Book"

    # Leader
    leader = formats[format]["Leader"]
    marcrecord.leader = leader

    # Identifier
    f001 = xmlrecord["header"]["identifier"][0]
    regexp = re.match("oai:digi.ub.uni-heidelberg.de:(\d+)", f001)
    if regexp:
        f001 = regexp.group(1)
        marcrecord.add("001", data="finc-107-" + f001)
    else:
        sys.exit("Der Identifier konnte nicht zerlegt werden: " + f001)

    # Zugangsart
    f007 = formats[format]["e007"]
    marcrecord.add("007", data=f007)

    # Periodizität
    year = get_field("dc:date")
    periodicity = formats[format]["008"]
    language = get_field("dc:language")
    f008 = marc_build_field_008(year, periodicity, language)
    marcrecord.add("008", data=f008)

    # Sprache
    language = get_field("dc:language")
    marcrecord.add("041", a=language)

    # Verfasser
    f100a = get_field("dc:creator")
    marcrecord.add("100", a=f100a)

    # Titel
    f245 = get_field("dc:title")
    marcrecord.add("245", a=f245)

    # Erscheinungsvermerk
    f260c = get_field("dc:date")
    publisher = ["b", "Hochschule Mittweida,", "c", f260c]
    marcrecord.add("260", subfields=publisher)

    # RDA-Inhaltstyp
    f336b = formats[format]["336b"]
    marcrecord.add("336", b=f336b)

    # RDA-Datenträgertyp
    f338b = formats[format]["338b"]
    marcrecord.add("338", b=f338b)

    # Schlagwörter
    f689a = get_field("dc:subject")
    if isinstance(f689a, list):
        for subject in f689a:
            if ";" not in subject:
                marcrecord.add("689", a=subject)
    else:
        f689a = f689a.split(" , ")
        if len(f689a) > 1:
            for subject in f689a:
                if ";" not in subject:
                    marcrecord.add("689", a=subject)

    # Link zu Datensatz und Ressource
    urls = get_field("dc:identifier")
    for url in urls:
        if "http" in url:
            marcrecord.add("856", q="text/html", _3="Link zum Datensatz", u=url)
        if "urn:" in url:
            marcrecord.add("856", q="text/html", _3="Link zur Ressource", u=url)

    # SWB-Inhaltstyp
    f935c = formats[format]["935c"]
    marcrecord.add("935", c=f935c)

    # Ansigelung und Kollektion
    marcrecord.add("980", a=f001, b="107", c="sid-107-col-heidelberg")

    outputfile.write(marcrecord.as_marc())

inputfile.close()
outputfile.close()
