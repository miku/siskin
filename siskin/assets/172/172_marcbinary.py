#!/usr/bin/env python3
# coding: utf-8

# SID 172
# https://projekte.ub.uni-leipzig.de/issues/14442


import sys
import marcx


# Default input and output
inputfilename = "172_input.tsv"
outputfilename = "172_output.mrc"

if len(sys.argv) == 3:
	inputfilename, outputfilename = sys.argv[1:]

inputfile = open(inputfilename, "r")
outputfile = open(outputfilename, "wb")

records = inputfile.readlines()

for line in records[1:]:
	
	fields = line.split("	")

	marcrecord = marcx.Record(force_utf8=True)
	marcrecord.strict = False
	
	# Leader
	marcrecord.leader = "     naa  22        4500"

	# ID
	f001 = fields[0]
	marcrecord.add("001", data="finc-172-" + f001)

	# 007
	marcrecord.add("007", data="cr")

	# Sprache
	f041a = fields[10]
	marcrecord.add("041", a=f041a)

	# 1. Schöpfer
	persons = fields[4].split("; ")
	f100a = persons[0]
	marcrecord.add("100", a=f100a)
	
	# Haupttitel	
	f245a = fields[6]	
	marcrecord.add("245", a=f245a)

	# Erscheinungsjahr
	f260c = fields[13]
	if len(f260c) == 5:	
		marcrecord.add("260", c=f260c)

	# Reihe
	f490a = fields[2]	
	marcrecord.add("490", a=f490a)

	# Rechtehinweis
	f500a = fields[3]	
	marcrecord.add("500", a=f500a)

	# Dateiformat
	f500a = fields[9]	
	marcrecord.add("500", a="Dateiformat: " + f500a)

	# Beschreibung
	f520a = fields[1]	
	marcrecord.add("520", a=f520a)

	# Schlagwörter
	f650a = fields[11]	
	marcrecord.add("650", a=f650a)

	# weitere Schöpfer
	for person in persons[1:]:
		marcrecord.add("700", a=person)
	
	# Link zur Ressource
	f856u = fields[12]	
	marcrecord.add("856", q="text/html", _3="Link zur Ressource", u=f856u)

	# Medienform
	marcrecord.add("935", b="cofz")

	# Kollektion
	marcrecord.add("980", a=f001, b="172", c="sid-172-col-opal")


	outputfile.write(marcrecord.as_marc())

outputfile.close()
