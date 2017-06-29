// FLUX 4.0.0
// Read XML (look for tagname "Record" foreach record), filter deleted records,
// morph to intermediate schema

// override default values if necessary
default MAP_DIR ="/assets/maps/";
default sid = "34";
default mega_collection = "PQDT Open";
default format = "ElectronicThesis";
default fileName = in;

fileName|
open-file|
decode-xml|
handle-generic-xml("Record")|
morph(FLUX_DIR + "morph.xml", *)|
encode-json|
write("stdout");
