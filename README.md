![bird drawing](http://i.imgur.com/PNq6dWf.gif)

siskin
======

Various tasks for metadata handling.

[![pypi version](http://img.shields.io/pypi/v/siskin.svg?style=flat)](https://pypi.python.org/pypi/siskin)


Getting started
---------------

You might want to try siskin in a [virtual environment](http://docs.python-guide.org/en/latest/dev/virtualenvs/) first.

    $ pip install siskin

Installation takes a few minutes and you'll need libxml, libxslt and mysql
development headers.

**Configuration**

Many siskin tasks require configuration. An example configuration file
can be found under [siskin.example.ini](https://github.com/miku/siskin/blob/master/siskin.example.ini).

Copy this file to `/etc/siskin/siskin.ini`. `BSZTask` will require an additional
configuration under `/etc/siskin/mappings.json`, which is not included in the
public distribution.

To use siskin, start the luigi scheduler first with `luigid` (in the background or in a separate terminal).

Siskin comes with a couple of [commands](https://github.com/miku/siskin/tree/master/bin), all sharing the prefix `task`:

    taskcat          - inspect task output
    taskdo           - run task
    taskhome         - show base directory of all artefacts
    taskindex-delete - delete Elasticsearch index and remove trace of it in the 'update_log'
    taskless         - inspect task output
    taskls           - show task output
    taskman          - manual of all tasks
    tasknames        - all task names
    taskoutput       - output file path of a task
    taskredo         - rm and do
    taskrm           - delete task artefact
    tasktree         - run tree command under the given tasks directory
    taskversion      - show siskin version
    taskwc           - count lines in task output

Run `taskman` to see what tasks are available. Run `taskdo` to execute a task.
Run `taskdo TASKNAME --help`, to see all available parameters of the task.


----

How does it sound? &mdash; Hear [The sound of data being processed](http://vimeo.com/99084953).
