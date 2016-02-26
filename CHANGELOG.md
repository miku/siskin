0.0.137 2016-02-26
------------------

* ai: switch to next pipeline
* ai: include gbi full texts
* ai: add templated filter config stub: AIFilterConfigNext
* ai: compress export
* crossref: docs
* gbi: remove obsolete tasks
* crossref: remove obsolete tasks
* ai: simplify span-tag config
* crossref: exclude blacklisted dois, where it's cheap
* crossref: fix CrossrefDOIAndISSNList
* ai: i/o gz
* ai: AIISSNCoverageReport, sort output
* doaj: fix issn extraction
* 85: expand, be verbose
* install: remove obsolete sudo
* ai: add gbi references
* remove OSX from install
* amsl: use tmp prefix
* ai: more stats on holdings, AICoverageISSN stub
* crossref: use unpigz
* crossref: prepare doi, issn list
* 85: add protocol hint, #6975
* docs: filterconf
* ai: 28, any
* ai: remove J59 with missing holding file for now
* ai: filterconf example
* add elsevier journals, 85
* ai: skip probably empty holding files from AMSL
* ai: skip suspiciously small files, when assembling config
* crossref: remove obsolete task
* install: various updates
* update README
* move config into etc
* install: udpate TODOs

0.0.135 2016-02-08
------------------

* crossref: plug over to CrossrefUniqItemsRedux
* crossref: explain, why not all records of the most recent harvest slice are present in the snapshot
* crossref: decompress input to work around broken pipes / write error 32
* crossref: CrossrefUniqItemsRedux filtering
* crossref: use tmpdir from config [Martin Czygan]
* crossref: compress and simplify [Martin Czygan]
* crossref: get rid of cat -n [Martin Czygan]
* crossref: CrossrefLineDOI, carry over filename [Martin Czygan]
* crossref: add CrossrefDOITable [Martin Czygan]
* crossref: allow empty fields, which indicate missing DOIs [Martin Czygan]
* crossref: source might have invalid lines [Martin Czygan]
* crossref: move towards chunk processing for smaller updates [Martin Czygan]
* crossref: remove unused tasks [Martin Czygan]
* amsl: allow both zipped and non-zipped kbart [Martin Czygan]
* amsl: move prefix into config [Martin Czygan]
* amsl: isil can have multiple downloads [Martin Czygan]
* amsl: api access, AMSLHoldingsFile, AMSLHoldingsISILList, AMSLHoldings [Martin Czygan]

0.0.134 2016-02-04
------------------

* ai: basic Coverage report
* thieme: gzip result
* crossref: number of attempts is not significant
* gbi: generate intermediate schema by kind
* gbi: fix zero output for update intermediate schema
* bartoc: urls are not stable
* common: add exclude_glob option to FTPMirror
* factbook: do not uncompress
* ssoar: fix whitespace
* ai: compress various intermediate results
* jstor: unpigz intermediate schema output
* gbi: adjust docs, dump/update db gap
* jstor: compress xml and intermediate schema
* add journaltocs
* gbi: extract some titles, refs. #6832
* crossref: docs
* gbi: update docs
* gbi: note on which axes a task operates, kind, update/dump
* gbi: do not carry the group, use more reliable DB names
* gbi: GBIDumpXML should not care about references or fulltext separation
* gbi: probably sufficient sigel-map
* gbi: GBIPackageMap
* gbi: note where the mappings come from, GBIDumpGroupList

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
