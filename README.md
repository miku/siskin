siskin
======

Various tasks for heterogeneous metadata handling. Based on
[luigi](https://github.com/spotify/luigi) from Spotify.

[![pypi version](http://img.shields.io/pypi/v/siskin.svg?style=flat)](https://pypi.python.org/pypi/siskin)

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

Evolving workflows
------------------

![](http://i.imgur.com/8bFvSvN.gif)
