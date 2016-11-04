default fileName = in;

fileName|
open-gzip|
as-lines|
decode-formeta|
morph(FLUX_DIR + "8986.xml", *)|
encode-json|
write("stdout");
