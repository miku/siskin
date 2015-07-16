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
* d:swarm - scalability issues, about 4M records per day


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
* https://github.com/miku/siskin/blob/master/docs/ai-intro/Vagrantfile
* https://asciinema.org/a/01i19zplnnisg9gfcn5s3kwta (13m)

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

It's on [pypi](https://pypi.python.org/pypi/siskin) (Python package index), you can install it with:

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
2. test them locally
3. if they work and are fast enough, commit and go to 1.
4. if they work, but are slow, find ways to speed up things

On occasion, run *AIExport* and *AIIntermediateSchema* and upload results into
SOLR and memcachedb as updates.


# DAG

The siskin package contains a couple of commands, all prefixed with
`task`, e.g. `taskdo`, `taskredo`, `taskrm`, `taskcat`, ...

A first look: https://asciinema.org/a/8hxtcpi1x72uzs3v63gy88m4m (6m)


# DAG

Advantages:

* most things happen in files, no database to setup or copy or share
* system is in a defined state most of the time (a task is either done or not)
* can evolve, start with some inefficient but working, improve over time


# CONVERSIONS

For larger file format conversions (e.g. Degruyter XML -> Intermediate Schema JSON)
Python alone is to slow.

For this performance critical parts I chose Go. These conversions are
mostly in a project named [span](http://github.com/miku/span).


# SPAN

span knows two commands:

* span-import
* span-export

Odd names, since there is no database or something.


# LICENSE HANDLING

span-export can work with holding files.

```sh
span-export -f DE-15:path/to/x.xml -f DE-14:path/to/y.xml -l DE-15-FID:/path/to/issn.list
```


# SOLR UPDATES

Little command line tool: [solrbulk](http://github.com/miku/solrbulk).

Uploads (indexes) documents into SOLR in bulk and in parallel.

----

People love it already:

> "Nice! This'll let me replace a (uglier) somewhat similar
   set of scripts I cludged together." 22.05.2015

> "awesome thanks! I keep hand rolling the same crappy utils that do the same thing,
   glad somebody finally got time to create something more generic!" 01.06.2015

----

If you need something like this for ES: [esbulk](http://github.com/miku/esbulk).


# BLOB UPDATES

Little command line tool: [memcldj](http://github.com/miku/memcldj).

Stands for memcache and line-delimited JSON.

Has retry built-in, so it usually runs fine with 100M records is one go.


# Summary

Install python package and little command line tools.

Create SOLR and intermediate schema files:

```sh
$ taskdo AIExport
$ taskdo AIIntermediateSchema
```

Upload to SOLR and blob:

```sh
$ solrbulk -host 173.19.112.21 -port 8085 -z \
    /media/mtc/Ether/siskin-data/ai/AIExport/date-2015-07-06.ldj.gz
$ memcldj -w 4 -verbose -key finc.record_id \
    -addr 173.19.112.98:12345 $(taskoutput AIIntermediateSchema)
```

Wait.


# Summary

Still things to do:

* I would love to see core luigi concepts implemented in Go, because this
  would simplify deployment.

* More documentation and rules for collaboration.

* ...

