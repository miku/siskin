// read XML (look for tagname "Record" foreach record), filter deleted records,
// morph to intermediate schema

// SID: 87

// override default values if necessary
default MAP_DIR ="/assets/maps/";
default sid = "87";
default mega_collection = "International Journal of Communication";
default fileName = in;

fileName|
open-file|
decode-xml|
handle-generic-xml("Record")|
morph(FLUX_DIR +  "morph.xml", *)|
encode-json|
write("stdout");
