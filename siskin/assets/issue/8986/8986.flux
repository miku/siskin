default fileName = in;

fileName|
open-file|
decode-formeta|
morph(FLUX_DIR + "8986.xml", *)|
encode-json|
write("stdout");
