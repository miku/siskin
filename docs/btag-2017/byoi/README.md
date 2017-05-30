README
======

This directory contains material for

> Heute baue ich meinen eigenen Artikelindex: Leichtgewichtige Metadatenverarbeitung

* http://www.professionalabstracts.com/api/iplanner/?conf=dbt2017&model=sessions&method=get&params[sids]=132&params[pids]=282&params[format]=pdf

Build an aggregated index including licensing and cross references with 225 lines of code.

Content
-------

```
.
├── [4.0K]  assets
│   ├── [ 518]  arxiv.flux
│   ├── [3.3K]  btoa.js
│   ├── [4.0K]  maps
│   │   ├── [ 151]  filemap_fincformat.tsv
│   │   ├── [ 232]  filemap_rft.genre.tsv
│   │   └── [1.3K]  sprachliste.tsv
│   └── [5.1K]  morph.xml
├── [1.2K]  deps.py
├── [4.0K]  inputs
│   ├── [ 66M]  arxiv.xml
│   ├── [100M]  crossref.ldj
│   ├── [ 26M]  de-14.tsv
│   ├── [ 26M]  de-15.tsv
│   ├── [ 15M]  doaj.ldj
│   ├── [1.3K]  iplanner.json
│   └── [8.1M]  mag.tsv.gz
├── [  72]  Makefile
├── [3.6K]  x00.py
├── [ 816]  x01.py
├── [ 943]  x02.py
├── [ 838]  x03.py
├── [1.1K]  x04.py
├── [ 751]  x05.py
├── [1015]  x06.py
├── [1.4K]  x07.py
├── [1.4K]  x08.py
└── [ 698]  x09.py

3 directories, 25 files
```

Task tree
---------

```
 \_ Export(format=solr5vu3)
    \_ TaggedIntermediateSchema()
       \_ IntermediateSchema()
	  \_ DOAJIntermediateSchema()
	     \_ DOAJInput()
	  \_ ArxivIntermediateSchema()
	     \_ ArxivInput()
	  \_ CrossrefIntermediateSchema()
	     \_ CrossrefInput()
       \_ CreateConfig()
```
