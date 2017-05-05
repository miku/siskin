default in = FLUX_DIR + "filmuni.mab";
default sid = "127";

in
| open-file
| as-records
| decode-mab
| filter(FLUX_DIR + "filter.xml")
| morph(FLUX_DIR + "morph.xml", *)
| stream-to-marc21xml
| write("stdout");
