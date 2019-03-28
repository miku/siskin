# siskin

Various tasks for heterogeneous metadata handling for Project
[finc](https://finc.info). Based on [luigi](https://github.com/spotify/luigi)
from Spotify.

[![pypi version](https://badge.fury.io/py/siskin.png)](https://pypi.python.org/pypi/siskin)

* Overview in a [few markdown slides](https://github.com/miku/siskin/blob/master/docs/ai-overview/slides.md)

Luigi (and other frameworks) allow to divide complex workflows into a set of
tasks, which form a
[DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph). The task logic is
implemented in Python, but it is easy to use external tools, e.g. via
[ExternalProgram](https://github.com/spotify/luigi/blob/master/luigi/contrib/external_program.py)
or [shellout](https://github.com/miku/gluish#easy-shell-calls). Luigi is
workflow glue and scales up (HDFS) and down (local scheduler).

More on Luigi:

* [Luigi docs](https://luigi.readthedocs.io/en/stable/)
* [Luigi presentation at LPUG 2015](https://github.com/miku/lpug-luigi)
* [Luigi workshop at PyCon Balkan 2018](https://github.com/miku/batchdata)

More on project:

* [Blog about index](https://finc.info/de/Archive/268) [de], 2015
* [Presentation at 4th VuFind Meetup](https://swop.bsz-bw.de/frontdoor/index/index/docId/1104) [de], 2015
* [Metadaten zwischen Autopsie und Automatisierung](https://www.bibliotheksverband.de/fileadmin/user_upload/Kommissionen/Kom_ErwBest/Tagungen/Erwkomm_Fortbild_Ddorf2018_Wiesenmueller.pdf#page=26) [de], 2018

## Install

```
$ pip install -U siskin
```

The siskin project includes a [bunch of
scripts](https://github.com/miku/siskin/tree/master/bin), that allow to create,
inspect or remove tasks or task artifacts.

Python 2 and 3 support is mostly done, rough edges might remain.

Run taskchecksetup to see, what additional tools might need to be installed
(this is a manually [curated](https://git.io/fhZvG) list, not everything is
required for every task).

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

## Update

For siskin updates a

```
$ pip install -U siskin
```

should suffices.

## Run

List tasks:

    $ tasknames

Run simple task:

    $ taskdo DOAJDump

Documentation:

    $ taskdocs | less -R

Remove artefacts of a task:

    $ taskrm DOAJDump

Inspect the source code of a task:

```python
$ taskinspect AILocalData
class AILocalData(AITask):
    """
    Extract a CSV about source, id, doi and institutions for deduplication.
    """
    date = ClosestDateParameter(default=datetime.date.today())
    batchsize = luigi.IntParameter(default=25000, significant=False)

    def requires(self):
        return AILicensing(date=self.date)
    ...
```

## Configuration

The siskin package harvests all kinds of data sources, some of which might be
protected. All credentials and a few other configuration options go into a
`siskin.ini`, either in `/etc/siskin/` or `~/.config/siskin/`. If both files
are present, the local options take precedence.

Luigi uses a bit of configuration as well, put it under `/etc/luigi/`.

Completions on task names will save you typing and time, so put
`siskin_compeletion.sh` under `/etc/bash_completion.d` or somewhere else.

```shell
$ tree etc
etc
├── bash_completion.d
│   └── siskin_completion.sh
├── luigi
│   ├── client.cfg
│   └── logging.ini
└── siskin
    └── siskin.ini
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

## Software versioning

Since siskin works mostly *on data*, software versioning differs a bit, but we
try to adhere to the following rules:

* *major* changes: *You need to recreate all your data from scratch*.
* *minor* changes: We added, renamed or removed *at least one task*. You will
  have to recreate a subset of the tasks to see the changes. You might need to change
  pipelines depending on those tasks, because they might not exist any more or have been renamed.
* *revision* changes: A modification within existing tasks (e.g. bugfixes).
  You will have to recreate a subset of the tasks to see this changes, but no new
  task is introduced. *No pipeline is broken, that wasn't already*.

These rules apply for version 0.2.0 and later. To see the current version, use:

```shell
$ taskversion
0.43.3
```

## Schema changes

To remove all files of a certain format (due to schema changes or such) it helps, if naming is uniform:

```shell
$ tasknames | grep IntermediateSchema | xargs -I {} taskrm {}
...
```

Apart from that, all upstream tasks need to be removed manually (consult the
[map](https://git.io/v5sdS)) as this is not automatic yet.

## Task dependencies

Inspect task dependencies with:

```shell
$ taskdeps JstorIntermediateSchema
  └─ JstorIntermediateSchema(date=2018-05-25)
      └─ AMSLService(date=2018-05-25, name=outboundservices:discovery)
      └─ JstorCollectionMapping(date=2018-05-25)
      └─ JstorIntermediateSchemaGenericCollection(date=2018-05-25)
```

Or visually via [graphviz](https://www.graphviz.org/).

```shell
$ taskdeps-dot JstorIntermediateSchema | dot -Tpng > deps.png
```

## Evolving workflows

![](http://i.imgur.com/8bFvSvN.gif)

## Development

* [commit-msg](https://raw.githubusercontent.com/miku/siskin/master/contrib/githooks.commit-msg) git hook for keeping issues and commits in line
* use [pylint](https://github.com/PyCQA/pylint), currently 9.18/10 with many errors ignored, maybe with [git commit hook](https://github.com/sebdah/git-pylint-commit-hook)
* use [pytest](https://docs.pytest.org/), [pytest-cov](https://pypi.org/project/pytest-cov/), coverage at 9%
* use [tox](https://tox.readthedocs.io/) for testing siskin Python 2 and 3 compatibility

## TODO

* [ ] The naming of the scripts is a bit unfortunate, `taskdo`, `taskcat`,
  .... Maybe better `siskin run`, `siskin cat`, `siskin rm` and so on.
* [ ] Investigate [pytest](https://docs.pytest.org/en/latest/) for testing tasks, given inputs.
