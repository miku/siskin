#!/usr/bin/env python3
# coding: utf-8

import io
import sys

import xlrd

import marcx

# Default input and output.
inputfilename, outputfilename = "99_input.xlsx", "99_output.mrc"

if len(sys.argv) == 3:
    inputfilename, outputfilename = sys.argv[1:]

outputfile = io.open(outputfilename, "wb")

workbook = xlrd.open_workbook(inputfilename)
sheet = workbook.sheet_by_name("Tabelle3")  # 08/2019

for i, row in enumerate(range(sheet.nrows), start=0):

    csv_record = [u"{}".format(v) for v in sheet.row_values(row)]
    marc_record = marcx.Record(force_utf8=True)

    if csv_record[0] == "authors":
        continue

    # Leader
    marc_record.leader = "     naa  22        4500"

    # ID
    f001 = str(i)
    marc_record.add("001", data="finc-99-" + f001)

    # 007
    marc_record.add("007", data="cr")

    #008
    f260c = csv_record[8]
    f260c = f260c.rstrip(".0")
    if len(f260c) == 4:
        marc_record.add("008", data=u"130227u%suuuuuxx uuup     c" % f260c)
    else:
        marc_record.add("008", data=u"130227uu20uuuuuuxx uuup     c")

    # ISSN
    f022a = csv_record[5]
    marc_record.add("022", a=f022a)

    # 1. Schöpfer
    f100a = csv_record[0]
    f100a = f100a.split("; ")
    f100a = f100a[0]
    marc_record.strict = False
    marc_record.add("100", a=f100a)
    marc_record.strict = True

    # Haupttitel
    f245a = csv_record[1]
    marc_record.add("245", a=f245a)

    # Verlag, Erscheinungsjahr
    f260b = csv_record[3]
    f260b = f260b.lstrip("Frankfurt: ")
    f260c = csv_record[8]
    f260c = f260c.rstrip(".0")
    publisher = ["a", "Frankfurt : ", "b", f260b + ", ", "c", f260c]
    marc_record.add("260", subfields=publisher)

    # Seitenzahl
    f300a = csv_record[11]
    if f300a != "":
        f300a = f300a.rstrip(".0")
        f300a = f300a + " S."
        marc_record.add("300", a=f300a)

    # weitere Schöpfer
    f700a = csv_record[0]
    f700a = f700a.split("; ")
    if len(f700a) > 1:
        for person in f700a[1:]:
            marc_record.add("700", a=person)

    # Quelle
    f773t = csv_record[2]
    issue = csv_record[6]
    issue = issue.rstrip(".0")
    volume = csv_record[7]
    volume = volume.rstrip(".0")
    year = csv_record[8]
    year = year.rstrip(".0")
    pages = csv_record[12]
    pagess = pages.rstrip(".0")
    f773g = "%s(%s)%s, S. %s" % (volume, year, issue, pages)
    marc_record.add("773", g=f773g, t=f773t)

    # Link zur Ressource
    f856u = csv_record[13]
    marc_record.add("856", q="text/html", _3="Link zur Ressource", u=f856u)

    # Medienform
    marc_record.add("935", b="cofz")

    # Kollektion
    marc_record.add("980", a=f001, b="99", c="sid-99-col-mediaperspektiven")

    outputfile.write(marc_record.as_marc())

outputfile.close()
