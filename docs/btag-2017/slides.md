Hands-On Lab: Build Your Own Index
==================================


> Heute baue ich meinen eigenen Artikelindex – Hands-On Workshop zum leichtgewichtigen Metadatenprozessing. 📎

106. Deutscher Bibliothekartag, 2017-06-02, Frankfurt am Main

[Martin Czygan](mailto:martin.czygan@uni-leipzig.de), [Tracy Hoffmann](mailto:tracy.hoffmann@uni-leipzig.de), [Leipzig University Library](https://ub.uni-leipzig.de)


> [https://ub.uni-leipzig.de](https://ub.uni-leipzig.de), [https://finc.info](https://finc.info), [https://amsl.technology](https://amsl.technology), [itprojekte@ub.uni-leipzig.de](mailto:itprojekte@ub.uni-leipzig.de)


----

About
=====

* Hands-On: We encourage participants to write code and run things on their machines.

* Lightweight: An overused term.

----

Why lightweight?
================

* allows reuse of existing programs and libraries
* not too much code
* transparent and reproducable

----

Some stats
==========

* code that runs the aggregated index build process at UBL  is about 5000 lines of code (lucene/index/IndexWriter.java)
* we reuse a lot of unix tools, we implement metafacture transformations, and custom tools (these are extra code, but they are also standalone tools)

----
