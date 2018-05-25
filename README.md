siskin
======

Various tasks for heterogeneous metadata handling for Project
[finc](https://finc.info). Based on [luigi](https://github.com/spotify/luigi)
from Spotify.

[![pypi version](https://badge.fury.io/py/siskin.png)](https://pypi.python.org/pypi/siskin)

* Overview in a [few markdown slides](https://github.com/miku/siskin/blob/master/docs/ai-overview/slides.md)

Install
-------

```
$ pip install -U siskin
```

Currently, Python 2 and 3 support is on the way and the installation might be a
bit flaky.

Run taskchecksetup to see, what needs to be installed.

```shell
$ taskchecksetup
ok     7z
ok     curl
ok     filterline
ok     flux.sh
ok     groupcover
ok     hurrly
ok     jq
ok     metha-sync
ok     microblob
ok     pigz
ok     solrbulk
ok     span-import
ok     unzip
ok     wget
ok     xmllint
ok     yaz-marcdump
```

Update
------

For siskin updates a

```
$ pip install -U siskin
```

should suffices.

Run
---

List tasks:

    $ tasknames

Run simple task:

    $ taskdo DOAJDump

Documentation:

    $ taskdocs | less -R

Remove artefacts of a task:

    $ taskrm DOAJDump

Configuration
-------------

The siskin package harvests all kinds of data sources, some of which might be
protected. All credentials and a few other configuration options go into
`siskin.ini`, either below `/etc/siskin/` or `~/.config/siskin/`.

Luigi uses a bit of configuration as well, put it under `/etc/luigi/`.

Completions on task names will save you typing and time, so put
`siskin_compeletion.sh` under `/etc/bash_completion.d` or somewhere else.

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

All configuration values can be inspected quickly with:

```
$ taskconfig
[core]
home = /var/siskin

[imslp]
listings-url = https://example.org/abc

[jstor]

ftp-username = abc
ftp-password = d3f
...
```

Software versioning
-------------------

Since siskin works mostly *on data*, software versioning differs a bit, but we
try to adhere to the following rules:

* *major* changes: *You need to recreate all your data from scratch*.
* *minor* changes: We added, renamed or removed *at least one task*. You will
  have to recreate a subset of the tasks to see the changes. You might need to change
  pipelines depending on those tasks, because they might not exist any more or have been renamed.
* *revision* changes: A modification within existing tasks (e.g. bugfixes).
  You will have to recreate a subset of the tasks to see this changes, but no new
  task is introduced. *No pipeline is broken, that wasn't already*.

These rules apply for version 0.2.0 and later.

Schema changes
--------------

To remove all files of a certain format (due to schema changes or such) it helps, if naming is uniform:

```shell
$ tasknames | grep IntermediateSchema | xargs -I {} taskrm {}
...
```

Apart from that, all upstream tasks need to be removed manually (consult the
[map](https://git.io/v5sdS)) as this is not automatic yet.

Task dependencies
-----------------

Inspect task dependencies with:

```
$ taskdeps JstorIntermediateSchema
  └─ JstorIntermediateSchema(date=2018-05-25)
      └─ AMSLService(date=2018-05-25, name=outboundservices:discovery)
      └─ JstorCollectionMapping(date=2018-05-25)
      └─ JstorIntermediateSchemaGenericCollection(date=2018-05-25)
```

Or visually via [graphviz](https://www.graphviz.org/).

```
$ taskdeps-dot JstorIntermediateSchema | dot -Tpng > deps.png
```


Evolving workflows
------------------

![](http://i.imgur.com/8bFvSvN.gif)
