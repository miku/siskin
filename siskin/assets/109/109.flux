default fileName = in;
default sid = "109";
default lookup = FLUX_DIR + "lookup_hierarchy.csv";
// default fileName = FLUX_DIR + "109.xml";
// default out = FLUX_DIR + "records.xml";

// first flow to create hierarchy lookup
fileName|
open-file|
decode-xml|
handle-marcxml|
filter(FLUX_DIR + "filter_relevant_for_FID.xml")|
morph(FLUX_DIR + "morph_lookup.xml")|
stream-to-triples|
template("${o}")|
write(lookup);

// second flow to transform data
fileName|
open-file|
decode-xml|
handle-marcxml|
filter(FLUX_DIR + "filter_relevant_for_FID.xml")|
morph(FLUX_DIR + "morph109.xml", *)|
stream-to-marc21xml|
write("stdout");
