// read XML (look for tagname "Record" foreach record), filter deleted records,
// morph to intermediate schema

// SID: 87

// override default values if necessary
default MAP_DIR ="/assets/maps/";
default sid = "87"; 
default mega_collection = "sid-87-col-intjcomm";
default fileName = in;
//default fileName = FLUX_DIR + sid + "records.xml";
//default out = FLUX_DIR + sid + ".json";

fileName|
open-file|
decode-xml|
handle-generic-xml("Record")|
morph(FLUX_DIR + sid + "_morph.xml", *)|
encode-json|
write("stdout");