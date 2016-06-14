README
======

This directory contains material for the ELAG 2016
bootcamp:

> Build your own aggregated discovery index of
  scholarly eresources

* [is.gd/nDh4TY](http://elag2016.org/index.php/program/bootcamps-june-6/build-your-own-aggregated-discovery-index-of-scholarly-eresources/)

----

The `code/` subdir contains python files. These
files demostrate two things:

* how to build a batch pipeline with luigi
* how to use an transform bibliographic data

During the bootcamp we will look at all
`scaffold_*py` files and fill them out.

The `extra/` contains some packages, like `span`
for data conversion, `jq` for JSON handling and
`vufind` as discovery system. It only serves as
documentation.

There are a few images used in the presentation
in the `images` directory.

The `input` directory contains raw input data for
Crossref, DOAJ and a KBART file export with
information about holdings for a single
institution.

----

The slides are saved under `slides.pdf` along with
the slide source.

Run `python hello.py` to check if python is
working as expected.

----

The vufind schema is altered slightly for this
presentation. The following fields have been
added:

```
<!-- quick-fix finc -->
<field name="imprint" type="string" indexed="true" stored="true" multiValued="true"/>
<field name="mega_collection" type="string" indexed="true" stored="true" multiValued="true"/>
<field name="source_id" type="string" indexed="true" stored="true" multiValued="true"/>
<field name="record_id" type="string" indexed="true" stored="true" multiValued="true"/>
<dynamicField name="format_*" type="string" indexed="true" stored="true"/>
<dynamicField name="*_facet" type="string" indexed="true" stored="true" multiValued="true"/>
```

----

The overall structure at the beginning of the
bootcamp should be like this:

```
byoi@elag:~/Bootcamp$ tree -sh
.
├── [4.0K]  code
│   ├── [ 998]  deps.py
│   ├── [  12]  __init__.py
│   ├── [ 406]  Makefile
│   ├── [ 438]  part0_helloworld.py
│   ├── [1.1K]  part1_require.py
│   ├── [2.4K]  part2_crossref.py
│   ├── [1.3K]  part3_doaj.py
│   ├── [1.0K]  part4_combine.py
│   ├── [2.0K]  part5_licensing.py
│   ├── [1.1K]  part6_export.py
│   ├── [ 465]  scaffold0_helloworld.py
│   ├── [1.5K]  scaffold1_require.py
│   ├── [2.3K]  scaffold2_crossref.py
│   ├── [1.2K]  scaffold3_doaj.py
│   ├── [1.1K]  scaffold4_combine.py
│   ├── [2.0K]  scaffold5_licensing.py
│   └── [1.1K]  scaffold6_export.py
├── [4.0K]  extra
│   ├── [ 640]  01-elag
│   ├── [2.7K]  install.sh
│   ├── [2.9M]  jq-linux64
│   ├── [1.4M]  solrbulk_0.1.5.4_amd64.deb
│   ├── [ 33M]  span_0.1.100_amd64.deb
│   ├── [  84]  vimrc
│   └── [139M]  vufind_3.0.deb
├── [ 270]  hello.py
├── [4.0K]  images
│   ├── [ 66K]  deps.png
│   ├── [ 124]  Makefile
│   ├── [ 254]  meta.dot
│   ├── [ 37K]  meta.png
│   ├── [ 908]  tour.dot
│   └── [118K]  tour.png
├── [4.0K]  input
│   ├── [4.0K]  crossref
│   │   ├── [123M]  begin-2015-01-01-end-2015-02-01-filter-deposit.ldj.gz
│   │   ├── [140M]  begin-2015-02-01-end-2015-03-01-filter-deposit.ldj.gz
│   │   └── [275M]  begin-2015-03-01-end-2015-04-01-filter-deposit.ldj.gz
│   ├── [4.0K]  doaj
│   │   └── [1.9G]  date-2016-02-01.ldj.gz
│   ├── [4.0K]  licensing
│   │   ├── [ 29M]  date-2016-04-19-isil-DE-15.tsv
│   │   └── [ 515]  filterconfig.json.template
│   ├── [ 471]  README.md
│   └── [4.0K]  warmup
│       ├── [  57]  hello.json
│       └── [523K]  input.json
├── [ 406]  Makefile
├── [1.7K]  README.md
├── [ 11K]  slides.md
├── [375K]  slides.pdf
└── [1.8K]  vm.sh

byoi@elag:~/Bootcamp$ du -sh
2.7G	.
```
