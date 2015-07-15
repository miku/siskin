# about:ai

![finc](img/finc.png)

MARTIN CZYGAN UBL 2015-07-16 10:00


# Roadmap

* a few DISCLAIMERS
* first things first
* short introduction to the DEPENDENCY GRAPH
* high performance FILE CONVERSIONS
* LICENSE HANDLING
* solr and memcachedb UPDATES


## Disclaimers

Most of the software here is work-in-progress.

With "project AI" UBL seems to try to act like a startup in a
corporate environment, which is cute.

![](img/blitz.png)

First commit:

> b5262ff 2015-01-06 | add crossref stub [Martin Czygan]

1090 commits across about 10 projects since then. That's about 5.5
commits per day - every day.

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

![](img/heart.png)

* nothing but ♥ ♥ ♥
* https://asciinema.org/a/2gpcfhapf4bxv7lya2voo3egh (1m)

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
```


## Dependencies

![](img/mtc.png)

The dependency graph is the overall structure, that allow us to build
complex workflows.

The python project is called [siskin](https://github.com/miku/siskin).

It's on pypi, you can install it with:

```sh
$ pip install siskin
```
