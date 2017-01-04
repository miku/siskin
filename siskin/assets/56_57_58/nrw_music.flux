default fileName = in;
default sid = sid;
//default fileName = FLUX_DIR + "mhe01_marc";
//default out = FLUX_DIR + "result2.xml";

fileName|
open-file|
decode-xml|
handle-marcxml|
morph(FLUX_DIR+ "nrw_music_morph.xml", *)| // one morph for sid 56,57,58
stream-to-marc21xml|
write("stdout");