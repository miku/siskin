#!/usr/bin/env python3
# coding: utf-8

import base64
import xlrd
import marcx

outputfile = open("88_output.mrc", "wb")

workbook = xlrd.open_workbook("88 RuG Aug 2017.xlsx")
sheet = workbook.sheet_by_name("Tabelle2")

for row in range(sheet.nrows):
	
	csv_record = sheet.row_values(row)
	marc_record = marcx.Record(force_utf8=True)

	if csv_record[0] == "rft.jtitle":
		continue

	# Leader
	marc_record.leader = "     naa  22        450 "

	# ID
	f001 = csv_record[13]
	f001 = f001.encode("utf-8")
	f001 = base64.b64encode(f001)
	f001 = f001.decode("ascii")
	marc_record.add("001", data="finc-88-%s" % f001)

	# 007
	marc_record.add("007", data="cr")

	# ISSN
	f022a = csv_record[4]
	marc_record.add("022", a=f022a)

	# 1. Schöpfer
	f100a = csv_record[1]
	f100a = f100a.split("; ")
	f100a = f100a[0]
	marc_record.strict = False
	marc_record.add("100", a=f100a)
	marc_record.strict = True

	# Haupttitel
	f245a = csv_record[10]
	f245a = str(f245a)
	marc_record.add("245", a=f245a)

	# Verlag, Erscheinungsjahr
	f260b = csv_record[2]
	f260b = str(f260b)
	f260c = csv_record[9]
	f260c = str(f260c).rstrip(".0")
	marc_record.add("260", b=f260b, c=f260c)
	
	# Seitenzahl
	f300a = csv_record[11]
	f300a = str(f300a).rstrip(".0")
	marc_record.add("300", a=f300a)

	# weitere Schöpfer
	f700a = csv_record[1]
	f700a = f700a.split("; ")
	if len(f700a) > 1:
		for person in f700a[1:]:
			marc_record.add("700", a=person)

	# Quelle
	f773t = csv_record[0]
	f773t = str(f773t)
	issue = csv_record[5]
	issue = str(issue).rstrip(".0")
	volume = csv_record[6]
	volume = str(volume).rstrip(".0")
	year = csv_record[9]
	year = str(year).rstrip(".0")
	pages = csv_record[12]
	pagess = str(pages).rstrip(".0")	
	f773g = "%s(%s)%s, S. %s" % (volume, year, issue, pages)
	marc_record.add("773", g=f773g, t=f773t)

	# Link zur Ressource
	f856u = csv_record[13]
	marc_record.add("856", q="text/html", _3="Link zur Ressource", u=f856u)

	# Medienform
	marc_record.add("935", b="cofz")

	# Kollektion
	marc_record.add("980", a=f001, b="88", c="Rundfunk und Geschichte")


	outputfile.write(marc_record.as_marc())

outputfile.close()