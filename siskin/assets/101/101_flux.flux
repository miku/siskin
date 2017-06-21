default inputfile = FLUX_DIR + "101_input.xml";
default outputfile = FLUX_DIR + "101_output.json";
default morphfile = FLUX_DIR + "101_morph.xml";
default filterfile = FLUX_DIR + "101_filter.xml";
default sid = "101";

inputfile
| open-file
| decode-xml
| handle-generic-xml("Tabelle1")
//| filter(filterfile)
| morph(morphfile, *)
| encode-json(prettyprinting="false")
| write("stdout");
// | write(outputfile);
