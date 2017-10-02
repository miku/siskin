#!/usr/bin/env python
# coding: utf-8

from builtins import *

import marcx
import xlrd

# Default input and output.
inputfilename = "100_Medienwissenschaft_Berichte_Papiere.xlsx"
outputfilename = "100_output.mrc"

if len(sys.argv) == 3:
	inputfilename, outputfilename = sys.argv[1:]

outputfile = open(outputfilename, "wb")

workbook = xlrd.open_workbook(inputfilename)
sheet = workbook.sheet_by_name("Tabelle1")

for i, row in enumerate(range(sheet.nrows), start=0):
	
	csv_record = sheet.row_values(row)
	marc_record = marcx.Record(force_utf8=True)

	if csv_record[0] == "authors":
		continue

	# Leader
	marc_record.leader = "     naa  22        4500"

	# ID
	f001 = str(i)
	marc_record.add("001", data="finc-100-" + f001)
	
	# 007
	marc_record.add("007", data="cr")

	#008
	f260c = csv_record[8]
	f260c = str(f260c).rstrip(".0")
	if len(f260c) == 4:
		marc_record.add("008", data="130227u%suuuuuxx uuup     c" % f260c)
	else:
		marc_record.add("008", data="130227uu20uuuuuuxx uuup     c")

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
	f245a = str(f245a)
	marc_record.add("245", a=f245a)

	# Verlag, Erscheinungsjahr
	f260b = csv_record[3]
	f260b = str(f260b)
	f260b = f260b.lstrip("Hamburg: ")
	f260c = csv_record[8]
	f260c = str(f260c).rstrip(".0")
	publisher = ["a", "Hamburg : ", "b", f260b + ", ", "c", f260c]
	marc_record.add("260", subfields=publisher)
	
	# Seitenzahl
	f300a = csv_record[11]
	f300a = str(f300a).rstrip(".0")
	marc_record.add("300", a=f300a)

	# weitere Schöpfer
	f700a = csv_record[0]
	f700a = f700a.split("; ")
	if len(f700a) > 1:
		for person in f700a[1:]:
			marc_record.add("700", a=person)

	# Quelle
	f773t = csv_record[2]
	f773t = str(f773t)
	issue = csv_record[6]
	issue = str(issue).rstrip(".0")
	year = csv_record[8]
	year = str(year).rstrip(".0")
	pages = csv_record[12]
	pagess = str(pages).rstrip(".0")	
	f773g = "(%s)%s, S. %s" % (year, issue, pages)
	marc_record.add("773", g=f773g, t=f773t)

	# Link zur Ressource
	f856u = csv_record[13]
	marc_record.add("856", q="text/html", _3="Link zur Ressource", u=f856u)

	# Medienform
	marc_record.add("935", b="cofz")

	# Kollektion
	values = ["a", f001, "b", "100", "c", 'Medienwissenschaft "Berichte und Papiere"']
	marc_record.add("980", subfields=values)

	outputfile.write(marc_record.as_marc())

outputfile.close()
