default inputfile = FLUX_DIR + "70_6.xml";
default outputfile = FLUX_DIR + "70_output.xml";
default morphfile = FLUX_DIR + "70_morph.xml";
default filterfile = FLUX_DIR + "70_filter.xml";
default sid = "70";

inputfile
| open-file
| decode-xml
| handle-generic-xml("_x0037_0_6")
| filter(filterfile)
| morph(morphfile, *)
| stream-to-marc21xml
| write(outputfile);