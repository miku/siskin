// read MARC21XML
// morph to intermediate schema

default MAP_DIR = "/assets/maps/";
default sid = "124";
default mega_collection = "DawsonEra";
default fileName = in;

fileName|
open-file|
decode-xml|
handle-marcxml|
morph(FLUX_DIR + sid + "_morph.xml", *)|
encode-json|
write("stdout");
