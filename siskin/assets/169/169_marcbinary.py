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

import collections
import re
import sys
import json
import marcx
import base64
import hashlib
import datetime
import tqdm


channels = ("3Sat", "ARD", "ARTE.DE", "ARTE.FR", "BR", "DW", "HR", "KiKA", "MDR", "NDR", "ORF", "PHOENIX", "RBB",
            "SR", "SRF", "SRF.Podcast", "SWR", "WDR", "ZDF", "ZDF-tivi", "3sat")

records = collections.defaultdict(dict)
seen = set()

inputfilename = "169_input.json" 
outputfilename = "169_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]
   
inputfile = open(inputfilename, "r")
outputfile = open(outputfilename, "wb")
content = inputfile.read()

pattern = re.compile(r"""^{"Filmliste":|,"X":|}$""")
lines = pattern.split(content)

for line in tqdm.tqdm(lines):

    # sort -u dups.ndj > uniq.ndj 
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

        hash = hashlib.sha1()
        fields = record["channel"] + record["topic"] + record["title"] + record["description"] + record["hr_duration"] + record["timestamp"] 
        hash.update(fields.encode("utf-8").strip())
        hash_record = hash.hexdigest()

        f001 = record["channel"] + record["topic"][:10] + record["title"][:20] + record["title"][-20:] + record["hr_duration"] + record["size"] + record["timestamp"]
        f001 = bytes(f001, "utf-8")
        f001 = base64.urlsafe_b64encode(f001)
        f001 = f001.decode("utf-8").rstrip("=")
        records[hash_record].update({"f001": f001})
       
        f245a = record["title"]
        records[hash_record].update({"f245a": f245a})

        timestamp = record["timestamp"]
        if timestamp:
            timestamp = int(timestamp)
            f260c = datetime.datetime.fromtimestamp(timestamp).strftime(", %Y")
        else:
            f260c = ""
        publisher = ["b", record["channel"], "c", f260c]
        records[hash_record].update({"f260": publisher})

        f306a = record["hr_duration"]
        records[hash_record].update({"f306a": f306a})

        if record["topic"] not in channels and " / " not in record["topic"] and record["topic"] != record["title"]:           
            records[hash_record].update({"f490a": record["topic"]})

        timestamp = record["timestamp"]
        if timestamp:
            timestamp = int(timestamp)
            f500a = datetime.datetime.fromtimestamp(timestamp).strftime("%d.%m.%Y um %H:%M Uhr")
            f500a = "Gesendet am " + f500a
            if "00:00:00" in f500a:
                f500a = f500a.replace("00:00:00 Uhr", "").replace(" um ", "")
            records[hash_record].update({"f500a": f500a})

        f520a = record["description"]
        records[hash_record].update({"f520a": f520a})
        
        if record["url_website"]:
            records[hash_record].setdefault("website", set()).add(record["url_website"])            
        if record["url_video_low"]:
            records[hash_record].setdefault("low", []).append(record["url_video_low"])
        if record["url_video"]:
            records[hash_record].setdefault("medium", []).append(record["url_video"])
        if record["url_video_hd"]:
            records[hash_record].setdefault("high", []).append(record["url_video_hd"])
    
for record in tqdm.tqdm(records.values(), total=len(records)):

    marcrecord = marcx.Record(force_utf8=True)
    marcrecord.strict = False
    marcrecord.leader = "     cam  22        4500"
    marcrecord.add("001", data="finc-169-" + record["f001"])           
    marcrecord.add("007", data="cr")
    marcrecord.add("245", a=record["f245a"])    
    marcrecord.add("260", subfields=record["f260"])   
    marcrecord.add("306", a=record["f306a"])
    marcrecord.add("490", a=record.get("f490a"))    
    marcrecord.add("500", a=record["f500a"])
    marcrecord.add("520", a=record["f520a"])

    for i, url in enumerate(record.get("website", []), start=1):
        if len(record["website"]) == 1:
            i = ""
        marcrecord.add("856", q="text/html", _3="Link zur Webseite %s" % i, u=url)

    for i, url in enumerate(record.get("low", []), start=1):
        if len(record["low"]) == 1:
            i = ""
        marcrecord.add("856", q="text/html", _3="Link zu Video %s (LD)" % i, u=url)

    for i, url in enumerate(record.get("medium", []), start=1):
        if len(record["medium"]) == 1:
            i = ""
        marcrecord.add("856", q="text/html", _3="Link zu Video %s (SD)" % i, u=url)

    for i, url in enumerate(record.get("high", []), start=1):
        if len(record["high"]) == 1:
            i = ""
        marcrecord.add("856", q="text/html", _3="Link zu Video %s (HD)" % i, u=url)
     
    marcrecord.add("935", b="cofz", c="vide")
    subfields = ["a", f001, "b", "169", "c", "MediathekViewWeb"]
    marcrecord.add("980", subfields=subfields)

    outputfile.write(marcrecord.as_marc())

inputfile.close()
outputfile.close()