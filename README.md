siskin
======

Various tasks for metadata handling.

[![pypi version](http://img.shields.io/pypi/v/siskin.svg?style=flat)](https://pypi.python.org/pypi/siskin)

Installation
------------

Python 2.7 required.

    $ pip install -U siskin

Add configuration files:

* `/etc/siskin/siskin.ini` - example: [siskin.example.ini](https://github.com/miku/siskin/blob/master/siskin.example.ini)
* `/etc/luigi/client.cfg` - docs: [luigi configuration](http://luigi.readthedocs.org/en/latest/configuration.html)

----

In progress: Installation script:

    $ curl -sL https://git.io/vg2iq | sudo sh

Run
---

List tasks:

    $ tasknames

Run task:

    $ taskdo AIUpdate

Evolving workflows
------------------

![](http://i.imgur.com/8bFvSvN.gif)
