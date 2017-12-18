// read MARC21XML
// morph to intermediate schema

// SID: 103

// override default values if necessary
default MAP_DIR ="/assets/maps/";
default sid = "30";
default mega_collection = "SSOAR Social Science Open Access Repository";
default fileName = in;
//default fileName = FLUX_DIR + "marcxml.xml";
//default out = FLUX_DIR + sid + ".json";

fileName|
open-file|
decode-xml|
handle-marcxml|
morph(FLUX_DIR + sid + "_morph.xml", *)|
encode-json|
write("stdout");
