#!/usr/bin/env python3
# coding: utf-8

"""
Mediaview, "Diary of a madman" (https://git.io/fATFM, https://git.io/fATNI).
Usage:
    $ python mediaview.py [FILE]
Fields:
    line_channel,    // 0
    line_topic,      // 1
    title,           // 2
    ,                // 3
    ,                // 4
    hr_duration,     // 5
    size,            // 6
    description,     // 7
    url_video,       // 8
    url_website,     // 9
    url_subtitle,    // 10
    ,                // 11
    url_video_low,   // 12
    ,                // 13
    url_video_hd,    // 14
    ,                // 15
    timestamp        // 16
The url_video_low, url_video_hd are encoded like "offset|s", where the final
url will be: url_video[offset] + s
"""


import re
import sys
import json
import marcx
import base64
import hashlib
import datetime


channels = ("3Sat", "ARD", "ARTE.DE", "ARTE.FR", "BR", "DW", "HR", "KiKA", "MDR", "NDR", "ORF", "PHOENIX", "RBB",
            "SR", "SRF", "SRF.Podcast", "SWR", "WDR", "ZDF", "ZDF-tivi", "3sat")


inputfilename = "169_input.json" 
outputfilename = "169_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]
   
inputfile = open(inputfilename, "r")
outputfile = open(outputfilename, "wb")
content = inputfile.read()

pattern = re.compile(r"""^{"Filmliste":|,"X":|}$""")
lines = pattern.split(content)

# sort -u dups.ndj > uniq.ndj 
seen = set()

for line in lines:
    if line in seen:
        print('dropping duplicate line: %s ...' % line[:40], file=sys.stderr)
        continue
    # XXX: up to a few 100000 lines this is ok
    seen.add(line)

    try:
        doc = json.loads(line)
    except Exception as exc:
        print(exc, file=sys.stderr) 
    else:
        if doc[0] != "":
            current_channel = doc[0]
        if doc[1] != "":
            current_topic = doc[1]
        record = {
            "channel": current_channel,
            "topic": current_topic,
            "title": doc[2],
            "hr_duration": doc[5],
            "size": doc[6],
            "description": doc[7],
            "url_video": doc[8],
            "url_website": doc[9],
            "url_subtitle": doc[10],
            "url_video_low": doc[12],
            "url_video_hd": doc[14],
            "timestamp": doc[16],
        }

        try:
            if record["url_video_low"] and "|" in record["url_video_low"]:
                offset, suffix = record["url_video_low"].split("|")
                record['url_video_low'] = record['url_video'][:int(offset)] + suffix
        except Exception as exc:
            print(exc, file=sys.stderr)

        try:
            if record["url_video_hd"] and "|" in record["url_video_hd"]:
                offset, suffix = record["url_video_hd"].split("|")
                record['url_video_hd'] = record['url_video'][:int(offset)] + suffix
        except Exception as exc:
            print(exc, file=sys.stderr)


        if record["timestamp"] == "":
            continue          

        marcrecord = marcx.Record(force_utf8=True)
        marcrecord.strict = False
        
        marcrecord.leader = "     cam  22        4500"
       
        f001 = record["channel"] + record["topic"][:10] + record["title"][:20] + record["title"][-20:] + record["hr_duration"] + record["size"] + record["timestamp"]
        f001 = bytes(f001, "utf-8")
        f001 = base64.b64encode(f001)
        f001 = f001.decode("utf-8").rstrip("=")
        marcrecord.add("001", data="finc-169-" + f001)
               
        marcrecord.add("007", data="cr")
        marcrecord.add("245", a=record["title"])
          
        timestamp = record["timestamp"]
        if timestamp != "":
            timestamp = int(timestamp)
            f260c = datetime.datetime.fromtimestamp(timestamp).strftime(", %d.%m.%Y, %H:%M:%S Uhr")
        else:
            f260c = ""
        publisher = ["b", record["channel"], "c", f260c]
        marcrecord.add("260", subfields=publisher)
        
        marcrecord.add("306", a=record["hr_duration"])

        if record["topic"] not in channels and " / " not in record["topic"] and record["topic"] != record["title"]:
            marcrecord.add("490", a=record["topic"])

        marcrecord.add("520", a=record["description"])

        if record["url_website"] != "":
            marcrecord.add("856", q="text/html", _3="Link zur Webseite", u=record["url_website"])
        if record["url_video_low"] != "":
            marcrecord.add("856", q="text/html", _3="Link zum Video (LD)", u=record["url_video_low"])
        if record["url_video"] != "":
            marcrecord.add("856", q="text/html", _3="Link zum Video (SD)", u=record["url_video"])
        if record["url_video_hd"] != "":
            marcrecord.add("856", q="text/html", _3="Link zum Video (HD)", u=record["url_video_hd"])
        if record["url_subtitle"] != "":
            marcrecord.add("856", q="text/html", _3="Link zum Video (Untertitel)", u=record["url_subtitle"])
      
        marcrecord.add("935", b="cofz", c="vide")

        collections = ["a", f001, "b", "169", "c", "MediathekViewWeb"]
        marcrecord.add("980", subfields=collections)
   
        outputfile.write(marcrecord.as_marc())

inputfile.close()
outputfile.close()