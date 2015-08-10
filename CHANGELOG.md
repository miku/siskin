0.0.119 2015-08-10
------------------

* ai: gzip AIIntermediateSchema
* crossref: parallel extraction messes up order; alternative fix: http://git.io/v3YZG
* ai: reduce gbi set, refs #5093
* crossref: add adhoc task, CrossrefMemberVsPublisher, refs #4833
* docs: update README, Vagrantfile
* scripts: various fixes in taskdeps-dot, taskdir, taskls, taskoutput, completion

0.0.118 2015-08-02
------------------

* ai: add [AIUpdate](http://git.io/vOZFg) wrapper task; include [DOIBlacklist](http://git.io/vOZFa) during [export](http://git.io/vOZFK)
* crossref: make jq dependency explicit
* docs: start rewriting README; add [AIExport](http://git.io/vOZFS) graph (plus [evolution](http://git.io/vOZFQ))
* new source: [DOI](http://git.io/vOZFj)
* setup: Makefile can upload; refuse to install on unsupported python versions
