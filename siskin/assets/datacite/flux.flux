// read XML (look for tagname "Record" foreach record), filter deleted records,
// morph to intermediate schema

// SID: TBA (datacite)

// override default values if necessary
default MAP_DIR ="/assets/maps/";
default sid = "datacite";
default mega_collection = "Datacite";
default format = "ElectronicArticle";
default fileName = in;

fileName|
open-gzip|
decode-xml|
handle-generic-xml("Record")|
morph(FLUX_DIR + "morph.xml", *)|
encode-json|
write("stdout");
