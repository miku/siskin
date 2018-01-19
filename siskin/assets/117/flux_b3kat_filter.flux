// DEPRECATED
default out = "stdout";

in|
open-file|
as-records|
catch-object-exception|
decode-marc21|
filter(FLUX_DIR + "filter_DE-B170.xml")|
stream-to-marc21xml|
write(out);
