default in = FLUX_DIR + "filmuni.mab";
default sid = "127";

in
| open-file
| as-records
| decode-mab
//| handle-generic-xml("datensatz")
| morph(FLUX_DIR + "morph.xml", *)
//| encode-formeta(style="multiline")
//| encode-marc21
| stream-to-marc21xml
| write("stdout");
