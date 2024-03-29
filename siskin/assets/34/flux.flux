// FLUX 4.0.0
// Read XML (look for tagname "record" foreach record), filter deleted records,
// morph to intermediate schema

// override default values if necessary
default MAP_DIR ="/assets/maps/";
default sid = "34";
default mega_collection = "ProQuest Open Access Dissertations and Theses (PQDT Open)";
default format = "ElectronicThesis";
default fileName = in;

fileName|
open-file|
decode-xml|
handle-generic-xml("record")|
morph(FLUX_DIR + "morph.xml", *)|
encode-json|
write("stdout");
