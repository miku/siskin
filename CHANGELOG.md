0.0.130 2015-12-04
------------------

gbi: do not use extra dir for syncing
ai: doaj adjust fid, refs. #6524

0.0.129 2015-12-01
------------------

* ai: doaj, add DE-540, refs: #5959

0.0.128 2015-11-26
------------------

* attach DE-15-FID to doaj, refs #6524, #4983
* amsl: remove obsolete SPARQL access

0.0.127 2015-11-20
------------------

* ai: use ISSN FID 20151005.csv
* amsl: use curl for AMSLCollections

0.0.126 2015-11-17
------------------

This is a canary.

* ssoar: add format parameter
* crossref: jq for messages (kind of slow)
* amsl: isil relations api example
* crossref: add CrossrefWorks tasks
* ssoar: fix imports
* the great refactoring continues
* a first round of updates (gluish, oai, es)
* bartoc: hardcode json dump url
* add source: bartoc, A multilingual, interdisciplinary Terminology Registry for Knowledge Organization Systems
* histbest: use oaimi, keep oai_dc for now
* ai: skip the first item
* ai: fix list
* replace gzip with pigz (ongoing)
* ai: use pigz
* mag: support separate downloads
* new source: mag
* arxiv: simplify harvest with oaimi
* update LICENCE
* thieme: test case for oaimi (https://github.com/miku/oaimi)
* crossref: allow sort to use more memory

0.0.125 2015-09-01
------------------

* doi: switch to yearly interval

0.0.124 2015-09-01
------------------

* ai: break down gbi for intermediate schema
* amls: stub, stub SPARQL query
* crossref: filter out some DOI (too) early, refs #5824
* docs: add another AIExport graph
* gbi: sync from gbi dropbox
* jstor: add DOI list, refs #5824
* thieme: monthly interval, fixes

0.0.123 2015-08-12
------------------

* bugfix: fix missing logdir attribute

0.0.122 2015-08-11
------------------

* scripts: fix typo

0.0.121 2015-08-11
------------------

* docs: do not use pip cache
* scripts: taskhome should fail more gracefully

0.0.120 2015-08-11
------------------

* setup: at install time urllib3 might not be available
* ai: detach degruyter from all but DE-15, refs #4731

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
