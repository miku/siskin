0.2.0 2017-09-06
----------------

* switch to a new versioning scheme, https://git.io/vF0QC

0.1.37 2017-08-03
-----------------

* new tasks, improvements and fixes
* metafacture / assets
* towards Python 2/3
* btag and documentation updates

0.1.12 2017-01-18
-----------------

* new tasks, improvements, fixes, across nrw, ai, crossref, arxiv, amsl

0.1.1 2016-06-16
----------------

* add DegruyterExport
* ai: AIDOIStats leads to MemoryError, TODO: use comm
* add AIDOIStats
* fix imports with isort
* crossref: add pipefail to shellout
* amsl: docs typo
* amsl: remove hard-wired 85 rules
* amsl: docs formatting
* degruyter: extract issns, if they exist
* ai: remove obsolete tasks
* taskpurge: cleanup
* pubmed: remove hard-coded dependence on FTP
* pylint checks
* taskps: save space
* align license headers
* cleanup docs
* docs: add link to VM
* docs: elag bootcamp skeleton
* docs: remove filterconfig
* docs: update for elag 2016
* cleanup example siskin.ini
* make config a property on the base task
* sources: docs
* update license headers
* update README

0.1.0 2016-06-13
----------------

* get rid of non-essential sources, now: 2661 SLOC

0.0.151 2016-04-12
------------------

* secure pypi uploads
* add .editorconfig
* update setup.py
* pubmed: some stats additions
* ignore generated vm packages
* docs: add note on vm packaging
* docs: remove sudo, since privileged is true by default (https://goo.gl/KGrIEs)
* docs: elag-vm: more RAM, mcrypt fix
* docs: add elag notes and vm provisioning script
* taskdeps-dot: render certain tasks with full parameters
* amsl, crossref: fix imports
* amsl: stub for filterconf from API
* amsl: remove obsolete task
* amsl: use a single AMSLService tasks with API realm and name parameters
* amsl: filter collection entries for a shard
* amsl: notes and TODO
* amsl: err on empty isil set
* amsl: collect ISILs that appear in the collecions list for a given shard, AMSLCollectionsISILList
* 85: intermediate schema combined (for blob)
* amsl: content file api stub

0.0.150 2016-03-30
------------------

* 85: switch to daily, refs. #6975
* 85: ad-hoc concat
* 85: another date template
* pubmed: stubs
* bin: docs
* taskps: grepable status
* update README
* taskps: handle broken pipe
* taskpurge: extend whitelist
* setup.py: add taskpurge
* yago: do not uncompress
* taskdo: catch missing task error
* add taskpurge command

0.0.149 2016-03-23
------------------

* add taskps command

0.0.148 2016-03-22
------------------

* crossref: add note on CrossrefChunkItems (HEAD, origin/master, origin/HEAD, master) [Martin Czygan]
* bin/taskrm: fix [ [Martin Czygan]
* update completion [Martin Czygan]
* cleanup and new command: taskinspect [Martin Czygan]
* bin/taskhead: fix whitespace [Martin Czygan]
* taskdocs: minimalist manual [Martin Czygan]
* update license headers [Martin Czygan]
* update logging.ini [Martin Czygan]
* update README [Martin Czygan]
* Makefile: add note on pypi [Martin Czygan]

0.0.147 2016-03-19
------------------

* bin/taskdu: fail if we cannot determine dir
* completion: remove obsolete command
* taskoutput: fix imports and friendlier reaction on spelling errors
* bin: towards more robust scripts
* bin/taskoutput: use luigis CmdlineParser
* bin/taskdo: friendlier missing parameter error
* completion: shells are not started in venvs; move tasknames into function

0.0.146 2016-03-19
------------------

* crossref: allow misconfiguration of crossref.doi-blacklist

0.0.145 2016-03-19
------------------

* crossref: remove direct dependency on DOIBlacklist, allow file to be configured
* ops: add sample crontab entry

0.0.144 2016-03-18
------------------

* install: test/installation for proxied env
* install: add -i, so possible proxy settings are used
* doi: remove doi tag, move tasks to crossref, for which this filter is used, anyway
* wos: adhoc analysis for oa office
* amsl: flip urikey
* crossref: add TODO
* ai: fix query
* ai: run queries with institition filter on
* ai: add gbi issns to AICoverageISSN
* ai: AIISSNCoverageCatalogMatches stub
* utils: encode utf-8
* gbi: add GBIISSNList
* wos: move CrossrefWebOfScienceMatch -> WOSCrossrefMatch
* wos: issn list
* crossref: process subs break pipes, sometimes
* ai: save some lines
* crossref: span-gh-dump is gone as well
* ai: iscov is deprecated
* gbi: solr-export by kind, stub
* jstor: fix whitespace
* jstor: fix key in JstorExport
* jstor: JstorExport stub, test per-source solr-importable artifacts
* amsl: warn explicitly about API changes
* gbi: shorten config by using package names only
* gbi: manually add KUSE
* contrib: helpers and preparation scripts, that we do not need to deploy
* gbi: add GBIDatabaseMap, generate dbmap for span (e.g. like https://git.io/v2ECx)
* install: add missing filterline

0.0.142 2016-03-08
------------------

* remove unused tests
* install: ubuntu: allow 2G RAM, xml etree gcc was killed with 1G?
* install: unify dir structure, move contrib to etc
* crossref: actually pass begin parameter
* amsl: skip non-kbart by name, refs. #7142
* ai: update docs
* crfix: accept missing keys
* crfix: stub, possible mitigation of https://github.com/CrossRef/rest-api-doc/issues/67
* ai: mark output as gzip

0.0.141 2016-03-04
------------------

* ai: fix output name and dummy AIDuplicatesSortbyDOI
* ai: add DE-Kn38 and use new amsl API
* ai: AIDuplicates, fix input key
* ai: dups stub
* ai: from span 0.1.75, span-solr is called span-export
* install: fix names
* docs: AIExport.png
* taskdeps-dot: do not visit deps twice

0.0.140 2016-03-02
------------------

* crossref: CrossrefCollectionsDifference stub, diffs AMSL and crossref collections
* install: add link to screencast
* install: shorten install script
* docs: add filtering pipeline image
* ai: CrossrefCollections, refs. #6985
* ai: adjust filterconf
* ai: prepare assembly from AMSL, AIFilterConfigNext, AIFilterConfigISIL
* amsl: AMSLCollectionsISIL, make shard a parameter
* amsl: AMSLCollectionsISIL stub
* utils: SetEncoder

0.0.139 2016-03-02
------------------

* ai: filterconf: 50, DE-15 only, w/o holdings
* ai: filterconf: use one subtree per source (48)
* ai: filterconf formatting
* ai: adjust filterconf template, refs. #7111
* taskhash: stub to report changed implementations of tasks (e.g. for recomputation)
* adhoc: simplify CrossrefWebOfScienceMatch
* 85: span-solr expects YYYY-MM-DD format
* adhoc: match a list of web of science dois against crossref
* bin: do not run tree, if task output needs parameters
* installation test: add centos-7.2
* ai: add todo regarding source switches
* crossref: add begin parameter to AIIntermediateSchema
* install: install pigz

0.0.138 2016-03-01
------------------

* 85: a direct approach to intermediate schema generation
* ai: fix extension
* install: skip python 2.7 installation, if already done
* install: do not overwrite existing config files

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
