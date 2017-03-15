// SID: 78

// override default values if necessary
default sid = "78";
default morphfile = FLUX_DIR + "78_morph.xml";
default filterfile = FLUX_DIR + "78_filter.xml";
default inputfile = in;
//default inputfile = FLUX_DIR + "izi.xml";
//default outputfile = FLUX_DIR + "result.json";
//default sid = "78";


inputfile 
| open-file
| decode-xml
| handle-generic-xml("Datensatz")
| filter(filterfile)
| morph(morphfile, *)
| encode-json(prettyprinting="false")
| write("stdout");