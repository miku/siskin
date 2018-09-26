#!/usr/bin/env python
# coding: utf-8

from __future__ import print_function

import io
import os
import re
import sys
import json
import base64

import marcx
import xmltodict


colormap = {
    "black and white": "Schwarzweiß",
    "black & white": "Schwarzweiß",
    "B&W": "Schwarzweiß",
    "color": "Farbe",
    "b&w": "Schwarzweiß",
    "Color": "Farbe",
    "Mix": "Schwarzweiß / Farbe",
    "B&W (tinded)": "Schwarzweiß (koloriert)",
    "b/w": "Schwarzweiß",
    "c": "Farbe",
    "tinted": "Schwarzweiß (koloriert)",
    "bw": "Schwarzweiß",
    "C": "Farbe",
    "B&W/C": "Schwarzweiß / Farbe",
    "tinted B&W": "Schwarzweiß (koloriert)",
    "B&W (tinted)": "Schwarzweiß (koloriert)",
    "sepia": "Farbe",
    "B/W": "Farbe",
    "color/B&W": "Schwarzweiß / Farbe",
    "b&w/color": "Schwarzweiß / Farbe",
    "Black and White": "Schwarzweiß",
    "B&w": "Schwarzweiß"
}

soundmap = {
    "black and white": "Schwarzweiß",
    "sound": "Ton",
    "silent": "Stumm",
    "No": "Stumm",
    "Yes": "Ton",
    "Sd": "Ton",
    "si": "Stumm",
    "Si": "Stumm",
    "silen": "Stumm",
    "sd": "Ton",
    "SD": "Ton"
}


def get_field(tag):
    try:
        return jsonrecord[tag]
    except:
        return ""


input_directory = "153"
outputfilename = "153_output.mrc"

if len(sys.argv) == 3:
    input_directory, outputfilename = sys.argv[1:]

outputfile = open(outputfilename, "wb")

for root, _, files in os.walk(input_directory):
    for filename in files:
        filepath = os.path.join(root, filename)
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
            if filename == "prelinger.ldj":
                f980c = "Internet Archive / Prelinger"
            elif filename == "classic_cartoons.ldj":
                f980c = "Internet Archive / Classic Cartoons"
            elif filename == "feature_films.ldj":
                f980c = "Internet Archive / Feature Films"
            elif filename == "more_animation.ldj":
                f980c = "Internet Archive / More Animation"
            elif filename == "vintage_cartoons.ldj":
                f980c = "Internet Archive / Vintage Cartoons"
            marcrecord.add("980", a=f001, b="153", c=f980c)

            outputfile.write(marcrecord.as_marc())

inputfile.close()
outputfile.close()
