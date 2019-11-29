# Installation

* [For users](#installation-for-users)
* [For developers](#installation-for-developers)

# Installation for users

* Goal: You want to execute tasks on Linux or MacOS. You do not want to develop.
* Prerequisites: Familiarity with the command line and basic Python tooling.

## Create a virtual environment (optional)

Depending on your preference and setup, you can use a [virtual
environment](https://docs.python.org/3/tutorial/venv.html) for this project.
Additional tools like
[virtualenvwrapper](https://virtualenvwrapper.readthedocs.io/en/latest/) can
simplify the handling of environments. [Conda](https://conda.io) comes with support for
[environments](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html)
built in.

## Install the siskin package

The *siskin* project is distributed as a Python package. The current release
should be available on [PyPI](https://pypi.org/project/siskin/) or on your
local Package repository (e.g. Nexus, artifactory, ...).

```
$ pip install -U --no-cache siskin
```

This will install siskin, its assets and a number of command line scripts to
interact with the task artifacts.

## Check additional requirements

While `pip` will install necessary Python dependencies, some tasks will use
external programs, such as [jq](https://stedolan.github.io/jq/),
[metafacture](https://github.com/metafacture/metafacture-core/wiki) and others.

To show, which additional programs are installed and which are still missing, you can run:

```
$ taskchecksetup
```

The output will look similar to this:

```
ok      7z
ok      csvcut
ok      curl
ok      filterline
ok      flux.sh
ok      groupcover
ok      hurrly
ok      jq
ok      marctexttoxml
ok      metha-sync
ok      microblob
ok      oaicrawl
ok      pigz
xx      solrbulk        http://github.com/miku/solrbulk/releases
ok      span-import
ok      unzip
ok      wget
ok      xmllint
ok      yaz-marcdump
```

In the left column you will see either an `ok` or an `xx`, where `ok` means
that the required program was found in your
[PATH](https://superuser.com/questions/284342/what-are-path-and-other-environment-variables-and-how-can-i-set-or-use-them).
If the program was not found in the
[PATH](https://superuser.com/questions/284342/what-are-path-and-other-environment-variables-and-how-can-i-set-or-use-them),
a link should be visible to the programs' homepage. Please install these program
manually, if necessary.

Note that not all tasks require external programs. In fact, you can try to run
any task just now. If an external program is missing, there will be an error,
which will include the name of the missing program.

### Metafacture

The siskin project currently uses Metafacture 4.0.0, which can be downloaded
from
[https://github.com/metafacture/metafacture-core/releases/tag/metafacture-runner-4.0.0](https://github.com/metafacture/metafacture-core/releases/tag/metafacture-runner-4.0.0).
Unarchive the distribution and symlink the `flux.sh` into your path.

```
~/bin/flux-4.0.0.sh -> ~/opt/metafacture-runner-4.0.0/flux.sh
~/bin/flux.sh -> flux-4.0.0.sh*
```

## Configuration

Siskin is based on [luigi](https://github.com/spotify/luigi). Luigi typically
uses [two configuration
files](https://luigi.readthedocs.io/en/stable/configuration.html):

* `/etc/luigi/luigi.conf`
* `/etc/luigi/logging.conf`

while siskin has a single configuration file: siskin.ini.

The siskin.ini file can reside in different locations:

* `/etc/siskin/siskin.ini`
* `$HOME/.config/siskin/siskin.ini`

These two locations are evaluated top down, meaning that a value defined in
both `/etc/siskin/siskin.ini` and `$HOME/.config/siskin/siskin.ini` the local
one at `$HOME/.config/siskin/siskin.ini` will take precedence.

The configuration file is divided into section.

```
[core]

tempdir = /tmp
metha-dir = /media/jupiter/.metha

[izi]
input = /tmp/izi-2019-02-07.xml
```

While siskin should be runnable out of the box, most tasks will require some
configuration, e.g. an input file path, username, password, FTP hostname and
similar information.

An annotated example configuration file can be found here:

* [https://github.com/miku/siskin/blob/master/etc/siskin/siskin.ini](https://github.com/miku/siskin/blob/master/etc/siskin/siskin.ini)

Again, not all sections are required for all tasks. There is one `core` section that defines a few general configuration entries:

```
[core]

# all artefacts will be stored under this directory
home = /tmp/siskin-data

# temporary dir (defaults to system default)
tempdir = /tmp

# metha dir, where OAI harvests will be cached
metha-dir = /tmp/.metha
```

## Listing all available tasknames

Installed and configured, one can list all possible task by listing their task names:

```
$ tasknames
AIApplyOpenAccessFlag
AIBlobDB
AICheckStats
AICollectionsAndSerialNumbers
AICoverageISSN
AIDOIList
AIDOIRedirectTable
AIDOIStats
AIErrorDistribution
AIExport
...
```

## Browsing task documentation

A short description of each task - autogenerated from the tasks' [docstring](https://www.python.org/dev/peps/pep-0257/) can be inspected via:

```
$ taskdocs
359 tasks found

AIApplyOpenAccessFlag
    Apply OA-Flag. Experimental, refs. #8986.

    1. Inhouse/Flag (per collection)
    2. KBART
    3. Gold-OA


AIBlobDB
    Create data.db file for microblob, then:

    $ microblob -db $(taskoutput AIBlobDB) -file $(taskoutput AIRedact) -serve


AICheckStats
    Run quality check and gather error records.

...
```

Note: If you pages, e.g. `less` looks a bit garbled, try to run:

```
$ taskdocs | less -r
```

## Running a task

In order to run a task you can use the `taskdo` command, the taskname and
optionally some task parameters need to be given:

```
$ taskdo DBInetIntermediateSchema --local-scheduler
```

Example for a task with a parameter (here: [ISIL](https://de.wikipedia.org/wiki/Bibliothekssigel#ISIL)):

```
$ taskdo AMSLHoldingsFile --isil DE-14 --local-scheduler
```

The local scheduler is useful for simple setups. For production use, the luigi
docs recommend the [central
scheduler](https://luigi.readthedocs.io/en/stable/central_scheduler.html).


## Inspecting task outputs

Each task that runs might produce some output. The output is usually a file. These
files will typically reside under the directory, which is configured in the
siskin configuration under `core.home` (the home variable in the core section).

To inspect the output path of task, you can run:

```
$ taskoutput DBInetIntermediateSchema
/media/titan/siskin-data/80/DBInetIntermediateSchema/output.ldj.gz
```

This does not mean, that the task output at this location already exists. To actually see, whether there is a file at that location, you can use:

```
$ taskls DBInetIntermediateSchema
-rw-rw-r-- 1 tir tir 2.6M May 24 17:59 /media/titan/siskin-data/80/DBInetIntermediateSchema/output.ldj.gz
```

This issues an `ls` command on the path output.

## Removing a task output

In order to remove a task output, one can use:

```
$ taskrm DBInetIntermediateSchema
```

This command is more or less equivalent to:

```
$ rm -f $(taskoutput DBInetIntermediateSchema)
```

So `taskrm` is just a shortcut so save you some time.

## Removing and recreating a task output in one command

Sometimes, one want to delete and recreate a task output. While one could write the following:

```
$ rm -f $(taskoutput DBInetIntermediateSchema)
$ taskdo DBInetIntermediateSchema
```

There is a short cut for this sequence as well, namely the `taskredo` script:

```
$ taskredo DBInetIntermediateSchema
```

## Inspecting all output of a given task

Sometimes, one wants to quickly glance at all files generated by given task. There is the `tasktree` command:

```
$ tasktree DOAJHarvest
/media/titan/siskin-data/28/DOAJHarvest
├── [5.2G]  date-2019-03-01.xml.gz
├── [2.8G]  date-2019-04-01.xml.gz
├── [2.9G]  date-2019-05-01.xml.gz
├── [3.0G]  date-2019-06-01.xml.gz
└── [3.1G]  date-2019-07-01.xml.gz

0 directories, 5 files
```

Again, this is just a shortcut for:

```
$ tree $(taskdir DOAJHarvest)
```

where `taskdir` is a command, that will emit the directory name, where the
output of the task will be stored.

### Upgrading siskin to a newer version

The siskin project can be upgraded with:

```
$ pip install -U --no-cache siskin
```

Important: The pip command will update the software and the software only. If a
new release contains e.g. a bugfix for a task, the output of that task need to
be recreated manually.

### Uninstall siskin

To uninstall siskin, run:

```
$ pip uninstall siskin
```

Note: this will not remove the data artifacts and configuration files. You will
have to remove the data artifacts and configuration files yourself.

# Installation for developers

* Goal: You want to run and develop tasks.

## Clone the repository

You can clone the `siskin` repository from a local git server or via:

```shell
$ git clone git@github.com:miku/siskin.git # or https://github.com/miku/siskin.git, git@git.sc.uni-leipzig.de:ubl/finc/index/siskin.git
```

## Setup a virtualenv (optional, recommended)

Set up a virtual enviroment.

> A cooperatively isolated runtime environment that allows Python users and
> applications to install and upgrade Python distribution packages without
> interfering with the behaviour of other Python applications running on the
> same system.

If you are not familiar with the concept of an environment, please take a moment to familiarize yourself with the concept:

* https://docs.python.org/3/glossary.html#term-virtual-environment
* https://docs.python.org/3/tutorial/venv.html

## Development install

In order to have siskin and all dependencies installed (in your virtual environment), run:

```
$ python setup.py develop
```

You can read a bit more about this command here:

* [Python setup.py develop vs install](https://stackoverflow.com/questions/19048732/python-setup-py-develop-vs-install)

Note: the following should have the same effect as well:

```
$ pip install -e .
```

You can check, whether you succeeded by trying to run any of the scripts
supplied by siskin, e.g. to check the current version:

```
$ taskversion
0.67.1
```

## Check additional requirements

This is the same as in the [USER install section](#check-additional-requirements).

## Configuration

This is the same as in the [USER install section](#configuration).

## Running tasks, inspecting output, removing artifacts

The handling of these tasks can be looked up in the user docs:

* [Listing all available tasknames](#listing-all-available-tasknames)
* [Browsing task documentation](#browsing-task-documentation)
* [Running a task](#running-a-task)
* [Inspecting task outputs](#inspecting-task-outputs)
* [Removing a task output](#removing-a-task-output)
* [Removing and recreating a task output in one command](#removing-and-recreating-a-task-output-in-one-command)
* [Inspecting all output of a given task](#inspecting-all-output-of-a-given-task)

## Developing and extending siskin

There are various typical tasks you might want to do:

* adding a new data source
* fixing a bug in an existing data source
* adding a new pipeline stage to an existing pipeline
* adding some new task (maybe independent of a data source, e.g. a report or analysis)
* adding or updating an asset (e.g. an XSLT stylesteet, a conversion script, ...)
* adding a new script (which reside under the `bin` directory)
* factoring out common functionality into a module (see e.g. `mail.py` or `mab.py` and the like)
* refactoring or reorganizing existing pipelines

Each of these tasks will require you to look at different files.

## Creating a source distribution

To create a [source distribution](https://packaging.python.org/glossary/#term-source-distribution-or-sdist) run:

```
$ python setup.py sdist
```

Or, as a shortcut:

```
$ make dist
```

This will create a tarball under the dist directory:

```
$ tree dist
dist
└── siskin-0.67.1.tar.gz

0 directories, 1 file
```

## Uploading a source distribution to a package repository

First, install [twine](https://pypi.org/project/twine/), for secure uploads.

```
$ pip install twine
```

To upload a source distribution PyPI (requires configured account), run:

```
$ make upload
```

To upload to a local python package index, configure your `~/.pypirc` file
accordingly. Here is an example configuration, using a package repository
called *internal*:

```
[distutils]
index-servers =
    internal
    pypi

[internal]
repository: https://example.com/nexus/repository/pypi-hosted/
username: user1234
password: pass1234
```

To upload to the internal repository, you can pass an argument to the Makefile:

```
$ make upload TWINE_OPTS='-r internal'
```

## More information

For additional information regarding development and deployment, please have a
look at the [README.md](README.md).

