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

should suffices. For a full update, run

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

Source complexity
-----------------

```shell
$ for f in $(find siskin/sources/ -name "*py"); do printf "$(git rev-list HEAD --count -- $f)       $f\n"; done | sort -nr
171	siskin/sources/crossref.py
134	siskin/sources/gbi.py
125	siskin/sources/amsl.py
52	siskin/sources/genios.py
47	siskin/sources/elsevierjournals.py
47	siskin/sources/doaj.py
46	siskin/sources/jstor.py
45	siskin/sources/degruyter.py
39	siskin/sources/thieme.py
33	siskin/sources/arxiv.py
20	siskin/sources/mag.py
19	siskin/sources/ssoar.py
19	siskin/sources/ieee.py
19	siskin/sources/dblp.py
18	siskin/sources/mhlibrary.py
15	siskin/sources/pubmed.py
15	siskin/sources/base.py
14	siskin/sources/highwire.py
13	siskin/sources/nrwmusic.py
9	siskin/sources/pqdt.py
9	siskin/sources/khm.py
8	siskin/sources/ijoc.py
7	siskin/sources/springer.py
7	siskin/sources/__init__.py
6	siskin/sources/zdb.py
4	siskin/sources/vkfilm.py
4	siskin/sources/hhbd.py
4	siskin/sources/dawson.py
3	siskin/sources/vkfilmff.py
3	siskin/sources/izi.py
3	siskin/sources/dummy.py
2	siskin/sources/kielfmf.py
2	siskin/sources/datacite.py
1	siskin/sources/zenodo.py
```
