default inputfile = FLUX_DIR + "119_marc.mrc";
default morphfile = FLUX_DIR + "119_morph.xml";
default outputfile = FLUX_DIR + "output.xml";

inputfile|
open-file|
as-records|
decode-marc21|
morph(morphfile)|
stream-to-marc21xml|
write(outputfile);