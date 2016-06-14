This directory contains various input data.

Crossref
--------

Deposited articles for Jan-Mar 2015. Around 5GB unzipped. The original data set
is about 120G.

```
$ for i in input/crossref/*gz; do unpigz -c $i; done | wc -c
5126751046
```

Around 4571700 items. A fraction of the complete set.

DOAJ
----

An elasticsearch dump from February 2016. Around 8GB.

```
$ wc -l  input/doaj/date-2016-02-01.ldj
2201212
```

Licensing information
---------------------

TODO.

