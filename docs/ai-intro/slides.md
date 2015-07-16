# about:ai

![finc](img/finc.png)

MARTIN CZYGAN UBL 2015-07-16 10:00


# Roadmap

* a few DISCLAIMERS
* CENTOS
* modeling workflows with a DEPENDENCY GRAPH
* FILE CONVERSIONS
* LICENSE HANDLING
* solr and memcachedb UPDATES


## Goal

Overview, not every detail.



## Disclaimers

Most of the software here is work-in-progress.

First commit:

> b5262ff 2015-01-06 | add crossref stub [Martin Czygan]

First demo 2015-01-28.

1090 commits across about 10 projects since then. That's about 5.5 commits per
day.

Finc SVN repository at the same time: 673 commits, 10 committers.


## Disclaimers

Can't you be more *entschleunigt*, you ask?

Have a look at:

http://www.slideshare.net/f.lohmeier/big-bibliographic-data-sca-ds-project-meeting-20150612/17

* "finc" - works fine with 100M records
* d:swarm - scalability issues, about 4M records per day!


## Disclaimers

I am really sorry, but ...

* the software will change,
* the update process will change and
* components will change.


## CentOS

* no Python 2.7
* EPEL for more packages

```sh
$ python -c "import sys; print(sys.version)"
2.6.6 (r266:84292, Nov 22 2013, 12:16:22)
[GCC 4.4.7 20120313 (Red Hat 4.4.7-4)]
```


## CentOS

* install Python 2.7 sensibly on CentOS 6.5
* https://asciinema.org/a/29915dmloarfkj2qkp6z7l5zm (13m)

After this process you have installed

* Python 2.7.9
* siskin and all dependencies

To make siskin work, we need a config file for FTP credential and such. Copy and adjust.

* https://asciinema.org/a/03hc9oo8dpyyc6j01gilta4l5 (30s)

```sh
$ sudo cp /vagrant/siskin.ini /etc/siskin/
$ sudo cp /vagrant/{client.cfg,logging.ini} /etc/luigi/
```


# DAG

The dependency graph (DAG) is the overall structure, that helps to build
complex workflows. Similar to a "make for data".

The python project is called [siskin](https://github.com/miku/siskin), and is based on [luigi](https://github.com/spotify/luigi).

It's on pypi (Python package index), you can install it with:

```sh
$ pip install siskin
```

Example graphs for AI:

* http://i.imgur.com/T1Ju4iF.png
* http://i.imgur.com/gdyC1sJ.png

Some more in the [docs](https://github.com/miku/siskin/tree/master/docs/catalog), e.g.
[DBPCategoryExtensionGND](https://github.com/miku/siskin/blob/master/docs/catalog/DBPCategoryExtensionGND.pdf).


# DAG

My development cycle:

1. write a few tasks in Python, Shell, etc., e.g. for Jstor, Thieme, ...
2. test them locally (next slide)
3. if they work and are fast enough, commit and go to 1.
4. if they work, but are slow, find ways to speed up things

On occasion, run "AIExport" and "AIIntermediateSchema" and upload results into
SOLR and memcachedb as updates.


# DAG

The siskin package contains a couple of commands, all prefixed with
"task", e.g. "taskdo", "taskredo", "taskrm", "taskcat", ...

A first look: https://asciinema.org/a/399w4eolzkfpiroavkfdewy6j (1m)
