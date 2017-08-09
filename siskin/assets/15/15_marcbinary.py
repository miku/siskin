#!/usr/bin/env python3
# coding: utf-8

import os
import io
import re
import marcx
import html

langmap = {
    "English": "eng",
    "German": "ger",
    "French": "fre",
    "Italian": "ita",
    "Russian": "rus",
    "Spanish": "spa",
    "Icelandic": "ice",
    "Polish": "pol",
    "Norwegian": "nor",
    "Latin": "lat",
    "Danish": "dan",
    "Finnish": "fin",
    "Welsh": "wel",
    "Swedish": "swe",
    "Neapolitan": "nap",
    "Hungarian": "hun",
    "Catalan": "cat",
    "Croatian": "hrv",
    "Czech": "cze",
    "Portuguese": "por",
    "Hebrew": "heb"
    }

outputfile = io.open("15_output.mrc", "wb")

for i, filename in enumerate(os.listdir("IMSLP"), start=1):

    if filename.endswith(".xml"):

        inputfile = io.open("IMSLP/" + filename, "r", encoding="utf-8")

        id = False
        title = False
        ptitle = False
        year = False
        language = False
        subject1 = False # Piece Style
        subject2 = False # Instrumentation
        subject3 = False # timeperiod
        composer = False
        librettist = False
        url = False

        f001 = ""
        f041a = ""
        f100a = ""
        f245a = ""
        f245b = ""
        f260c = ""
        f590a = ""
        f590b = ""
        f689a = ""
        f700a = ""
        f856u = ""
        f950a = []

        for line in inputfile:

            regexp = re.search("<recordId>(.*)<\/recordId>", line)
            if regexp:
                f001 = regexp.group(1)
                continue

            ##############################################

            if language == True:
                regexp = re.search("<string>(.*)<\/string>", line)
                if regexp:
                    language = regexp.group(1)
                    regexp = re.search("^(.*?)\W.*", language)
                    if regexp:
                        f041a = regexp.group(1)
                    else:
                        f041a = language
                    f041a = langmap.get(f041a, "")
                    if f041a == "":
                        print("Die Sprache %s fehlt in der Map!" % language)
                        f041 = "eng"

            regexp = re.search("name=\"Language\"", line)
            if regexp:
                language = True
                continue
            else:
                language = False

            ##############################################

            if composer == True:
                regexp = re.search("<string>(.*)<\/string>", line)
                if regexp:
                    f100a = regexp.group(1)

            regexp = re.search("name=\"composer\"", line)
            if regexp:
                composer = True
                continue
            else:
                composer = False

            ##############################################

            if title == True:
                regexp = re.search("<string>(.*)<\/string>", line)
                if regexp:
                    f245a = regexp.group(1)
                    f245a = html.unescape(f245a)

            regexp = re.search("name=\"worktitle\"", line)
            if regexp:
                title = True
                continue
            else:
                title = False

            ##############################################

            if ptitle == True:
                regexp = re.search("<string>(.*)<\/string>", line)
                if regexp:
                    f245b = regexp.group(1)
                    f245b = html.unescape(f245b)

            regexp = re.search("name=\"Alternative Title\"", line)
            if regexp:
                ptitle = True
                continue
            else:
                ptitle = False

            ##############################################

            if year == True:
                regexp = re.search("<string>(\d\d\d\d)<\/string>", line)
                if regexp:
                    f260c = regexp.group(1)
                else:
                    f260c = ""

            regexp = re.search("name=\"Year of First Publication\"", line)
            if regexp:
                year = True
                continue
            else:
                year = False

            ##############################################

            if librettist == True:
                regexp = re.search("<string>\[\[:Category:(.*?)\|.*<\/string>", line)
                if regexp:
                    f700a = regexp.group(1)

            regexp = re.search("name=\"Librettist\"", line)
            if regexp:
                librettist = True
                continue
            else:
                librettist = False

            ##############################################

            if subject1 == True:
                regexp = re.search("<string>(.*)<\/string>", line)
                if regexp:
                    s = regexp.group(1)
                    s = s.title()
                    f950a.append(s)
                    f590a = s

            regexp = re.search("name=\"Piece Style\"", line)
            if regexp:
                subject1 = True
                continue
            else:
                subject1 = False

            ##############################################

            if subject2 == True:
                regexp = re.search("<string>(.*)<\/string>", line)
                if regexp:
                    s = regexp.group(1)
                    s = s.title()
                    f950a.append(s)
                    f590b = s

            regexp = re.search("name=\"Instrumentation\"", line)
            if regexp:
                subject2 = True
                continue
            else:
                subject2 = False

            ##############################################

            if subject3 == True:
                regexp = re.search("<string>(.*)<\/string>", line)
                if regexp:
                    f950a.append(regexp.group(1))

            regexp = re.search("name=\"timeperiod\"", line)
            if regexp:
                subject3 = True
                continue
            else:
                subject3 = False

            ##############################################

            if url == True:
                regexp = re.search("<string>(.*)<\/string>", line)
                if regexp:
                    f856u = regexp.group(1)

            regexp = re.search("name=\"permlink\"", line)
            if regexp:
                url = True
                continue
            else:
                url = False

        marcrecord = marcx.Record(force_utf8=True)
        marcrecord.strict = False
        marcrecord.leader = "     ncs  22        450 "
        marcrecord.add("001", data="finc-15-%s" % f001) # f001 ergänzen!
        marcrecord.add("007", data="qu")
        marcrecord.add("008", data="130227uu20uuuuuuxx uuup%s  c" % f041a)
        marcrecord.add("041", a=f041a)
        marcrecord.add("100", a=f100a)
        marcrecord.add("245", subfields=["a", f245a, "b", f245b])
        marcrecord.add("260", b=f260c)
        marcrecord.add("590", subfields=["a", f590a, "b", f590b])
        marcrecord.add("700", a=f700a)

        subtest = []
        for subject in f950a:
            if subject not in subtest:
               subtest.append(subject)
               marcrecord.add("689", a=subject)

        marcrecord.add("856", q="text/html", _3="Petrucci-Musikbibliothek", u=f856u)

        #subtest = []
        #for subject in f950a:
        #    if subject not in subtest:
        #        subtest.append(subject)
        #        marcrecord.add("950", a=subject)

        marcrecord.add("950", y=f260c)

        marcrecord.add("970", c="PN")

        marcrecord.add("980", a=f001, b="15", c="Petrucci-Musikbibliothek")

        outputfile.write(marcrecord.as_marc())

        inputfile.close()

    #print(i)
    #if i == 10000:
    #    break


outputfile.close()



