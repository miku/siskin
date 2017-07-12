default inputfile = FLUX_DIR + "DE-B170.mrc";
default outputfile = FLUX_DIR + "117_output.xml";
default morphfile = FLUX_DIR + 117_morph.xml;

dump|
open-file|
decode-xml|
handle-marcxml|
filter(FLUX_DIR + "filter1_relevant_for_FID.xml")|
filter(FLUX_DIR + "filter2_relevant_for_FID.xml")|
morph(morphfile)|
stream-to-marc21xml|
write(out);