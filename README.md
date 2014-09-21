![bird drawing](http://i.imgur.com/PNq6dWf.gif)

siskin
======

Various tasks for metadata handling.

Getting started
---------------

You might want to try siskin in a [virtual environment](http://docs.python-guide.org/en/latest/dev/virtualenvs/) first.

    $ git clone git@github.com:miku/siskin.git
    $ cd siskin
    $ python setup.py install

Installation takes a few minutes and you'll need libxml, libxslt and mysql
development headers.

**Configuration**

Some siskin tasks require configuration. An example configuration file
can be found under [siskin.example.ini](https://github.com/miku/siskin/blob/master/siskin.example.ini).

Copy this file to `/etc/siskin/siskin.ini`. `BSZTask` will require an additional
configuration under `/etc/siskin/mappings.json`, which is not included in the
public distribution.

To use siskin, start the luigi scheduler first with `luigid` (in the background or in a separate terminal).

    $ luigid --background

A task that should work without any further configuration is a wikipedia related tasks.
For example to extract all raw mediawiki citations (beware, they look ugly)
from the German wikipedia, try:

    $ taskdo WikipediaRawCitations --language de

Initially this will take some time, since a wikipedia dump must be downloaded first.

Data Stores
-----------

siskin uses [elasticsearch](http://elasticsearch.org/) as its primary data store. Usually, each data source
has its own index. Most data sources come with a task to build an up-to-date
version of their index, and they - by convention - have the suffix Index, like
[`DOABIndex`](https://github.com/miku/siskin/blob/6897c0c4d4ea483f3a0b5bc5df6ad821a8c8e296/siskin/sources/doab.py#L89),
[`OSOIndex`](https://github.com/miku/siskin/blob/6897c0c4d4ea483f3a0b5bc5df6ad821a8c8e296/siskin/sources/oso.py#L178),
[`VIAFIndex`](https://github.com/miku/siskin/blob/6897c0c4d4ea483f3a0b5bc5df6ad821a8c8e296/siskin/sources/viaf.py#L184),
[`NEPIndex`](https://github.com/miku/siskin/blob/6897c0c4d4ea483f3a0b5bc5df6ad821a8c8e296/siskin/sources/nep.py#L450), etc.

The BSZ indexing task is awkwardly named [`BSZIndexPatch`](https://github.com/miku/siskin/blob/6897c0c4d4ea483f3a0b5bc5df6ad821a8c8e296/siskin/sources/bsz.py#L1517), because it actually
can patch the BSZ index to be up-to-date by deleting obsolete docs and only
indexing the docs, that are not in the index yet or have been updated. For
about 6 million records, a typical daily patch takes about 10 minutes on single
machine elasticsearch cluster. (It will probably be renamed into `BSZIndex` in the future
to follow convention.)

For BSZ processing, siskin makes use of a couple of other (internal) data stores.

elasticsearch is used mainly for two kinds of operations. To query data sources for
certain data, e.g.
[`GNDDefinitions`](https://github.com/miku/siskin/blob/6897c0c4d4ea483f3a0b5bc5df6ad821a8c8e296/siskin/sources/gnd.py#L444) will extract all definitions, that are in the GND,
[`DOIList`](https://github.com/miku/siskin/blob/6897c0c4d4ea483f3a0b5bc5df6ad821a8c8e296/siskin/workflows/adhoc.py#L23) will extract in a best effort manner all DOIs from BSZ, etc. The second
use cases for elasticsearch is fuzzy deduplication. The lucene *more-like-this*
query facility is key here. The task
[`FuzzyCandidates`](https://github.com/miku/siskin/blob/6897c0c4d4ea483f3a0b5bc5df6ad821a8c8e296/siskin/workflows/fuzzy.py#L139) can generate a list of records from several indices, that are similar and probably duplicates.

There is no central relation database, although some tasks may use sqlite3
[iternally](https://github.com/miku/siskin/search?utf8=%E2%9C%93&q=sqlite3db), to speed up operations and to allow SQL queries over certain data.

Tools
-----

siskin often uses shell command in tasks, both for simplicity and speed. Some
programs are installed by default on UNIX systems, like `awk`, `sed`, `perl`, `scp`, `grep`, `sort`, `unzip`, `tar`, etc.
Additionally, siskin uses other programs, mostly for MARC, elasticsearch and linked data related tasks, that are not installed by default.
Among them are [xsltproc](http://xmlsoft.org/XSLT/xsltproc.html), [php](http://php.net/), [yaz](http://www.indexdata.com/yaz), [marctools](https://github.com/ubleipzig/marctools), [estab](https://github.com/miku/estab), [ntto](https://github.com/miku/ntto), [cayley](https://github.com/google/cayley) and a few more.
Most tasks will let you know by themselves, what additional programs they need, and how to get them:

    $ # Running a task with a missing external program ...
    $ taskdo EBLJson
    ...
    ... Worker running Executable(name=marctojson, message='http://git.io/1LXpQA')
    ... Worker failed Executable(name=marctojson, message='http://git.io/1LXpQA')
    Traceback (most recent call last):
      File "/home/mtc/.virtualenvs/siskin/local/lib/python2.7/site-packages/...
        self.task.run()
      File "/home/mtc/.virtualenvs/siskin/local/lib/python2.7/site-packages/...
        self.message))
    RuntimeError: External app marctojson required.
    http://git.io/1LXpQA

A rough list of extrernal tools used:

    frequency command                 package name
    --------- -------                 ------------
    23        cat            .
    21        marctojson     x        marctools
    18        wget           .
    15        yaz-marcdump   .
    11        marcuniq       x        marctools
    10        sort           .
    10        cayley         x        cayley
     9        scp            .
     9        awk            .
     8        marctotsv      x        marctools
     8        estab          x        estab
     7        rsync          .
     7        ntto           x        ntto
     7        grep           .
     7        curl           .
     6        xsltproc       .
     6        tar            .
     5        marcmap        x        marctools
     4        unzip          .
     4        cut            .
     3        serdi          x        serdi
     3        gunzip         .
     2        touch          .
     2        tac            .
     2        marcdb         x        marctools
     1        wikitojson     x        wikitools
     1        wikinorm       x        wikitools
     1        wikidatatojson x        wikitools
     1        wikicats       x        wikitools
     1        stardust       x        stardust
     1        sed            .
     1        php            .
     1        pbzip2         .
     1        mpg123         .
     1        marcxmltojson  x        marctools
     1        esmlt          x        esmlt
     1        egrep          .
     1        7z             .
     1        bash           .

Commands
--------

Siskin comes with a couple of [commands](https://github.com/miku/siskin/tree/master/bin), all sharing the prefix `task`:

    taskcat          - inspect task output
    taskcp           - copy task output
    taskdir          - show task directory
    taskdo           - run task
    taskdu           - show disk usage of task
    taskhome         - show base directory of all artefacts
    taskindex-delete - delete Elasticsearch index and remove trace in 'update_log'
    taskless         - inspect task output
    taskls           - show task output
    taskman          - manual of all tasks
    tasknames        - all task names
    taskoutput       - output file path of a task
    taskredo         - rm and do
    taskrm           - delete task artefact
    taskstatus       - show whether a task is done or not
    tasktree         - run tree command under the given tasks directory
    taskversion      - show siskin version
    taskwc           - count lines in task output

Run

    taskman

to see what tasks are available. Run `taskdo` to execute a task. Run

    taskdo TASKNAME --help

to see all available parameters of the task.

----

How does it sound? &mdash; Hear [The sound of data being processed](http://vimeo.com/99084953).
