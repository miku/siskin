// DEPRECATED
default out = "stdout";

in|
open-file|
decode-xml|
handle-marcxml|
filter(FLUX_DIR + "filter1_relevant_for_FID.xml")|
filter(FLUX_DIR + "filter2_relevant_for_FID.xml")|
morph(FLUX_DIR + "117_morph.xml")|
stream-to-marc21xml|
write(out);
