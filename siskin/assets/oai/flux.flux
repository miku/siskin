// Read XML (look for tagname "Record" foreach record), filter deleted records,
// morph to intermediate schema

// override default values if necessary
default MAP_DIR ="/assets/maps/";
default sid = "TODO: set source id";
default mega_collection = "TODO: set collection name";
default format = "ElectronicArticle";
default fileName = in;

fileName|
open-gzip|
decode-xml|
handle-generic-xml("Record")|
morph(FLUX_DIR + "morph.xml", *)|
encode-json|
write("stdout");
