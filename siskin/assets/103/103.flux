// liest XML ein (Trennung der einzelnen Records durch "Record"), uebergibt an morph und speichert in json
// eine kleiner kommentar

// die default Werte werden beim Aufruf des Skripts �berschrieben
default MAP_DIR ="/home/tracy/maps/";
default sid = "103";
default mega_collection = "Margaret Herrick Library";
// default fileName = FLUX_DIR + "sid" + sid + "records.xml";
// default out = FLUX_DIR + "sid" + sid + ".json";
default fileName = in;

fileName|
open-file|
decode-xml|
handle-generic-xml("Record")|
// morph(FLUX_DIR + "sid" + sid + "morph.xml", *)|
filter(FLUX_DIR + sid + "filter.xml")|
morph(FLUX_DIR + sid + "_morph.xml", *)|
//encode-json(prettyprinting="true")|
encode-json|
write("stdout");
