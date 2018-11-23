#!/usr/bin/env python
# coding: utf-8

"""
Archive.org to MARC. Allows variable number of inputs.

    $ python 153_marcbinary.py OUTPUT INPUT [INPUT, ...]

Post-process link check via [clinker](https://github.com/miku/clinker), 17min
for about 12000 records.

    $ time marctojson -p $(taskoutput ArchiveMARC) | \
        jq -c -r '.["856"][] | .["u"][] | {"url": .}' | \
        clinker -w 16 > 153.report.ndj

    $ jq .status 153.report.ndj | sort | uniq -c | sort -nr                                                                                                      │·
    12801 200

"""

from __future__ import print_function

import base64
import glob
import io
import json
import os
import re
import sys

import marcx
import xmltodict


colormap = {
   u"black and white": u"Schwarzweiß",
   u"black & white": u"Schwarzweiß",
   u"B&W": u"Schwarzweiß",
   u"color": u"Farbe",
   u"b&w": u"Schwarzweiß",
   u"Color": u"Farbe",
   u"Mix": u"Schwarzweiß / Farbe",
   u"B&W (tinded)": u"Schwarzweiß (koloriert)",
   u"b/w": u"Schwarzweiß",
   u"c": u"Farbe",
   u"tinted": u"Schwarzweiß (koloriert)",
   u"bw": u"Schwarzweiß",
   u"C": u"Farbe",
   u"B&W/C": u"Schwarzweiß / Farbe",
   u"tinted B&W": u"Schwarzweiß (koloriert)",
   u"B&W (tinted)": u"Schwarzweiß (koloriert)",
   u"sepia": u"Farbe",
   u"B/W": u"Farbe",
   u"color/B&W": u"Schwarzweiß / Farbe",
   u"b&w/color": u"Schwarzweiß / Farbe",
   u"Black and White": u"Schwarzweiß",
   u"B&w": u"Schwarzweiß"
}

soundmap = {
    u"black and white": u"Schwarzweiß",
    u"sound": u"Ton",
    u"silent": u"Stumm",
    u"No": u"Stumm",
    u"Yes": u"Ton",
    u"Sd": u"Ton",
    u"si": u"Stumm",
    u"Si": u"Stumm",
    u"silen": u"Stumm",
    u"sd": u"Ton",
    u"SD": u"Ton"
}


def get_field(tag):
    try:
        return jsonrecord[tag]
    except:
        return ""

def remove_tags(field):
    if isinstance(field, str):
        return re.sub("\<.*?\>", "", field)
    else:
        return ""


# input_directory = "153"
outputfilename = "153_output.mrc"
input_filenames = glob.glob('153/*.ldj')

if len(sys.argv) > 2:
    outputfilename, input_filenames = sys.argv[1], sys.argv[2:]

outputfile = open(outputfilename, "wb")

for filepath in input_filenames:
    filename = os.path.basename(filepath)
    inputfile = io.open(filepath, "r", encoding="utf-8")

    for line in inputfile:

        jsonobject = json.loads(line)

        try:
            jsonrecord = jsonobject["metadata"]
        except:
            print(filename, file=sys.stderr)
            continue

        marcrecord = marcx.Record(force_utf8=True)

        # Leader
        marcrecord.leader = "     cam  22        4500"

        # Identifier
        try:
            f001 = jsonrecord["identifier"]
        except:
            continue

        f001 = f001.encode("utf-8")
        f001 = base64.b64encode(f001)
        f001 = f001.decode("ascii")
        f001 = f001.rstrip("=")
        marcrecord.add("001", data="finc-153-" + f001)

        # Format
        marcrecord.add("007", data="cr")

        # Urheber
        f110a = get_field("creator")
        if f110a != "" and f110a != "Unknown":
            marcrecord.add("110", a=f110a)

        # Hauptitel
        f245a = get_field("title")
        if "delete" in f245a or "Delete" in f245a:
            continue
        marcrecord.add("245", a=f245a)

        # gestattet leere Felder, solange der Titel und der Identifier vorhanden sind
        marcrecord.strict = False

        # Filmstudio
        f260b = get_field("publisher")

        # Erscheinungsjahr
        f260c = get_field("date")
        if f260c != "":
            regexp = re.search("(\d\d\d\d)", f260c)
            if regexp:
                f260c = regexp.group(1)

        # Verlag
        if f260b != "" and f260b == f110a:
            f260b = ""  # verhindert, dass Körperschaft und Verlag nebeneinander im Katalog angezeigt werden, wenn beide identisch sind

        if f260b != "" and f260c != "":
            f260b = f260b + ", "  # ergänzt das Trennzeichen zwischen Produktionsfirma und Jahr, wenn beides vorhanden
        publisher = ["b", f260b, "c", f260c]
        marcrecord.add("260", subfields=publisher)

        # Spielzeit
        runtime = get_field("runtime")

        # Bild
        color_old = get_field("color")
        if color_old != "":
            color = colormap.get(color_old, "")
        else:
            color = ""
        if color == "" and color_old != "":
            print("Die Farbe %s wurde in der Colormap nicht gefunden" % color_old, file=sys.stderr)

        # Ton
        sound_old = get_field("sound")
        if sound_old != "":
            sound = soundmap.get(sound_old, "")
        else:
            sound = ""
        if sound == "" and sound_old != "":
            print("Der Sound %s wurde in der Soundmap nicht gefunden" % sound_old, file=sys.stderr)

        if color != "" and sound != "":
            f300b = color + " + " + sound
        elif color != "":
            f300b = color
        elif sound != "":
            f300b = sound
        else:
            f300b = ""

        if runtime != "":
            runtime = runtime.lstrip("00:")
            runtime = runtime.lstrip("0:")
            runtime = " (" + runtime
            if "min." not in runtime:
                runtime = runtime + " min.)"
            else:
                runtime = runtime + ")"
            f300a = f300b + runtime
        else:
            f300a = f300b

        marcrecord.add("300", a=f300a)

        # Spielzeit (extra MARC-Feld)
        f306a = get_field("runtime")
        if f306a != "":
            f306a = f306a.lstrip("00:")
            f306a = f306a.lstrip("0:")
            if "min" not in f306a:
                f306a = f306a + " min."
        marcrecord.add("306", a=f306a)

        # Soundformat (extra MARC-Feld)
        marcrecord.add("344", a=sound)

        # Bildformat (extra -MARC-Feld)
        marcrecord.add("346", a=color)

        # Annotation
        f520a = get_field("description")
        f520a = remove_tags(f520a)
        marcrecord.add("520", a=f520a)

        # Schlagwörter
        subjects = jsonrecord.get("subject", "")
        if subjects != "":
            if isinstance(subjects, list):
                for subject in subjects:
                    subject = subject.title()
                    marcrecord.add("650", a=subject)
            else:
                if subjects != "need keyword":
                    subject = subjects.title()
                    subjects = subject.split(";")
                    if isinstance(subjects, list):
                        for subject in subjects:
                            subject = subject.title()
                            marcrecord.add("650", a=subject)
                    else:
                        marcrecord.add("650", a=subject)


        # hebt die Erlaubnis für leere Felder wieder auf
        marcrecord.strict = True

        # Link zur Ressource
        f856u = get_field("identifier")
        marcrecord.add("856", q="text/html", _3="Link zur Ressource", u="https://archive.org/details/" + f856u)

        # Medienform
        marcrecord.add("935", b="cofz", c="vide")

        # Kollektion
        if "prelinger" in jsonrecord["collection"]:
            f980c = "sid-153-prelinger"
        elif "classic_cartoons" in jsonrecord["collection"]:
            f980c = "sid-153-col-classiccartoons"
        elif "feature_films" in jsonrecord["collection"]:
            f980c = "sid-153-col-featurefilms"
        elif "more_animation" in jsonrecord["collection"]:
            f980c = "sid-153-col-moreanimation"
        elif "vintage_cartoons" in jsonrecord["collection"]:
            f980c = "sid-153-col-vintagecartoons"
        else:
            raise ValueError('collection mapping missing: %s', jsonrecord['collection'])

        marcrecord.add("980", a=f001, b="153", c=f980c)

        try:
            outputfile.write(marcrecord.as_marc())
        except UnicodeDecodeError as exc:
            raise ValueError("%s: %s" % (marcrecord["001"].value(), exc))

    inputfile.close()

outputfile.close()
