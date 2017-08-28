siskin
======

Various tasks for heterogeneous metadata handling.

[![pypi version](http://img.shields.io/pypi/v/siskin.svg?style=flat)](https://pypi.python.org/pypi/siskin)

* Overview in a [few markdown slides](https://github.com/miku/siskin/blob/master/docs/ai-overview/slides.md)

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

Schema changes
--------------

To remove all files of a certain format (due to schema changes or such) it helps, if naming is uniform:

```shell
$ tasknames | grep IntermediateSchema | xargs -I {} taskrm {}
...
```

Apart from that, all upstream tasks need to be removed manually (consult the
[map](https://git.io/v5sdS)) as this is not automatic yet.

Evolving workflows
------------------

![](http://i.imgur.com/8bFvSvN.gif)
