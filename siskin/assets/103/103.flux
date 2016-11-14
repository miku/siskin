// read XML (look for tagname "Record" foreach record), filter deleted records,
// morph to intermediate schema

// SID: 103

// override default values if necessary
default MAP_DIR ="/assets/maps/";
default sid = "103";
default mega_collection = "Margaret Herrick Library";
default fileName = in;
// default fileName = FLUX_DIR + "sid" + sid + "records.xml";
// default out = FLUX_DIR + "sid" + sid + ".json";


fileName|
open-file|
decode-xml|
handle-generic-xml("Record")|
filter(FLUX_DIR + sid + "filter.xml")|
morph(FLUX_DIR + sid + "_morph.xml", *)|
encode-json|
write("stdout");
