![bird drawing](http://i.imgur.com/PNq6dWf.gif)

siskin
======

Various tasks for metadata handling.

[![pypi version](http://img.shields.io/pypi/v/siskin.svg?style=flat)](https://pypi.python.org/pypi/siskin)


Getting started
---------------

You might want to try siskin in a [virtual environment](http://docs.python-guide.org/en/latest/dev/virtualenvs/) first.

Installation takes a few minutes and you'll need libxml, libxslt and mysql
development headers.

    $ pip install siskin

Most siskin tasks require configuration. An example configuration file
is can be found under [siskin.example.ini](https://github.com/miku/siskin/blob/master/siskin.example.ini).

Copy this file to `/etc/siskin/siskin.ini`. `BSZTask` will require an additional
configuration under `/etc/siskin/mappings.json`, which is not included in the
public distribution.

Siskin comes with a couple of commands, all sharing the prefix `task`:

    taskcat          - inspect task output
    taskdo           - run task
    taskless         - inspect task output
    taskls           - show task output
    taskman          - manual of all tasks
    tasknames        - all task names
    taskoutput       - output file path of a task
    taskredo         - rm and do
    taskrm           - delete task artefact
    taskwc           - count lines in task output

Run `taskman` to see what tasks are available. Run `taskdo` to execute a task.
Run `taskdo TASKNAME --help`, to see all available parameters of the task.
