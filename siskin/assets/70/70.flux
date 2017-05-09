default inputfile = FLUX_DIR + "70.xml";
default outputfile = FLUX_DIR + "output.xml";
default morphfile = FLUX_DIR + "morph.xml";
default filterfile = FLUX_DIR + "filter.xml";
default sid = "70";

inputfile
| open-file
| decode-xml
| handle-generic-xml("_x0037_0_3")
| filter(filterfile)
| morph(morphfile, *)
| stream-to-marc21xml
| write(outputfile);