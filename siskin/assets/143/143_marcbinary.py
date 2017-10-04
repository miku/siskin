#/usr/bin/env python
# coding: utf-8

import marcx
import pymarc

oldrecords = pymarc.marcxml.parse_xml_to_array('143_input.xml')
outputfile = open("143_output.mrc", "wb")

for oldrecord in oldrecords:
    
    if not oldrecord["001"] or not oldrecord.leader:
	    continue

    newrecord = marcx.Record(force_utf8=True) 

    # Leader
    leader = oldrecord.leader
    newrecord.leader = "     %s  22        4500" % leader[5:8]
    
    # ID
    f001 = oldrecord["001"].data
    newrecord.add("001", data="finc-143-%s" % f001)
    
    # 008
    f008 = oldrecord["008"].data
    f008 = f008.replace("\"", " ")
    newrecord.add("008", data=f008)
    
    # 035
    f035a = oldrecord["035"]["a"]
    newrecord.add("035", a=f035a)

    # 040
    f040a = oldrecord["040"]["a"]
    newrecord.add("040", a=f040a)

    # 1. Schöpfer
    f100a = oldrecord["100"]["a"]
    newrecord.add("100", a=f100a)
    
    # Haupttitel, Titelzusatz
    f245a = oldrecord["245"]["a"]
    f245b = oldrecord["245"]["b"]
    newrecord.add("245", a=f245a, b=f245b)

    # Erscheinungsort, Verlag, Erscheinungsjahr
    f260a = oldrecord["260"]["a"]
    f260b = oldrecord["260"]["b"] 
    f260c = oldrecord["260"]["c"]
    publisher = ["a", f260a, "b", f260b, "c", f260c]
    newrecord.add("260", subfields=publisher) 

    # Umfangsangabe
    f300a = oldrecord["300"]["a"]
    newrecord.add("300", a=f300a)

    # Reihe
    f490a = oldrecord["490"]["a"]
    newrecord.add("490", a=f490a)

    # allgemeine Fußnote
    f500a = oldrecord["500"]["a"]
    newrecord.add("500", a=f500a)

    # Zusammenfassung
    f520a = oldrecord["520"]["a"]
    newrecord.strict = False
    newrecord.add("520", a=f520a)
    newrecord.strict = True

    # Zielgruppe
    try:
    	f521a = oldrecord["521"]["a"]
    	newrecord.add("521", a=f521a)
    except:
    	pass

    # Sprachvermerk
    try:
    	f546a = oldrecord["546"]["a"]
    	newrecord.add("546", a=f546a)
    except:
    	pass

    # Schlagwort
    try:
    	f689a = oldrecord["650"]["a"]
    	newrecord.add("689", a=f689a)
    except:
    	pass

    # URL
    try:
    	f856u = oldrecord["856"]["u"]
        newrecord.add("856", q="text/html", _3="Link zur Ressource", u=f856u)
    except:
    	continue

    # Kollektion
    f980a = "finc-143-%s" % f001
    newrecord.add("980", a=f980a, b="143", c="JOVE")   
    

    outputfile.write(newrecord.as_marc())

outputfile.close()
