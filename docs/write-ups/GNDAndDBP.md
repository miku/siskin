On GND and DBPedia
==================

If not otherwise metioned the following versions are used for all analysis.

GND
---

* Opendata Homepage: http://datendienst.dnb.de/cgi-bin/mabit.pl?userID=opendata&pass=opendata&cmd=login
* [GND.ttl.gz](http://datendienst.dnb.de/cgi-bin/mabit.pl?cmd=fetch&userID=opendata&pass=opendata&mabheft=GND.ttl.gz) as of 2014-10-17

DBPedia
-------

* Version 2014, as can be found under http://data.dws.informatik.uni-mannheim.de/dbpedia/2014

Interlinking
------------

```
$ taskdo GNDDBPediaLinks
$ taskwc GNDDBPediaLinks
$ taskdo DBPGNDLinks --version 2014 --language de --base http://data.dws.informatik.uni-mannheim.de/dbpedia
$ taskdo DBPGNDLinks --version 2014 --language en --base http://data.dws.informatik.uni-mannheim.de/dbpedia

```

There are 80665 `owl:sameAs` relations in the GND data.

There are 259558 `<http://de.dbpedia.org/property/gnd>` relations in the German DBPedia,
in file `...2014/de/nt/infobox_properties_de.nt`.

In the English DBPedia, in file `...2014/en/nt/infobox_properties_en.nt`,
the relation is called `<http://dbpedia.org/property/gnd>`,
of which there are 7873 occurences.

In the German DBPedia, of the 259558 possible links, 57916 are linking
to invalid GNDs due to type errors. 2 entries are invalid due to other reasons,
201640 are valid GND.
