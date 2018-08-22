// DEPRECATED(2018-08-15): Use span-import -h hhbd
// liest XML ein (Trennung der einzelnen Records durch "Record"), uebergibt an morph und speichert in json
// die default Werte werden beim Aufruf des Skripts ueberschrieben

default sid = "107"; 
default mega_collection = "Heidelberger historische Bestände";
default in = FLUX_DIR + "" + sid + "records.xml";
default out = FLUX_DIR + "sid" + sid + ".json";

in|
open-file|
decode-xml|
handle-generic-xml("Record")|
morph(FLUX_DIR + "morph.xml", *)|
//encode-json(prettyprinting="true")|
encode-json|
write("stdout");
