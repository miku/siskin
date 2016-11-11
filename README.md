siskin
======

Various tasks for heterogeneous metadata handling.

[![pypi version](http://img.shields.io/pypi/v/siskin.svg?style=flat)](https://pypi.python.org/pypi/siskin)

Install
-------

Tested on vanilla CentOS 6.7, 7.1, 7.2 and Precise 64 boxes:

    $ curl -sL https://git.io/vg2iq | sudo -i bash

Update
------

For siskin updates a

```
$ pip install -U siskin
```

suffices. For a full update, run

```
$ curl -sL https://git.io/vg2iq | sudo -i bash
```

again.

Run
---

List tasks:

    $ tasknames

Run simple task:

    $ taskdo DOAJDump

Documentation:

    $ taskdocs | less -R

Configure
---------

The installation script adds basic config files. Edit them.

```
#   $ tree etc
#   etc
#   ├── bash_completion.d
#   │   └── siskin_completion.sh
#   ├── luigi
#   │   ├── client.cfg
#   │   └── logging.ini
#   └── siskin
#       └── siskin.ini
```

Evolving workflows
------------------

![](http://i.imgur.com/8bFvSvN.gif)

Monitor progress
----------------

* with `taskps` - https://asciinema.org/a/chm7kexyplj0tqalqqirz7dyh
