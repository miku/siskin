Hands-On Lab: Build Your Own Index
==================================


> Heute baue ich meinen eigenen Artikelindex â€“ Hands-On Workshop zum leichtgewichtigen Metadatenprozessing. ðŸ“Ž

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

* code that runs the aggregated index build process at UBL is about 5000 lines of code
* we reuse a lot of existing tools, we implement metafacture transformations, and custom tools (these are extra code, but they are also standalone)

----

Luigi
=====

* a so-called *second generation* ETL tool
* similar projects: airflow, oozie, askaban, pachyderm
* not so much about analysis (like spark), more about orchestration

----

Luigi
=====

* very simple to get started, only a few primitives
* extensible
* used by many companies, LOC seems to [use luigi as well](http://digitalpreservation.gov/meetings/dcs16/DChudnov-MGallinger_LCLabReport.pdf)

----

Orchestration
=============

* what should happen when

----

Tradeoffs
=========

* DSL vs programming language
* Python is relatively simple to learn and widely used in science and industry

Pro DSL
=======

* fast, specialized tool
* easier to learn

Pro programming language
========================

* no need to work around limitations
* performance, deployment, automation

----

