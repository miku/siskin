// Read XML (look for tagname "Record" foreach record)
// Morph to intermediate schema.

// SID: 121

// override default values if necessary
default MAP_DIR ="/assets/maps/";
default sid = "121";
default mega_collection = "Arxiv";
default format = "ElectronicArticle";
default fileName = in;
// default fileName = FLUX_DIR + sid + "records.xml";
// default out = FLUX_DIR + sid + ".json";

fileName|
open-file|
decode-xml|
handle-generic-xml("Record")|
morph(FLUX_DIR + "morph.xml", *)|
encode-json|
write("stdout");
