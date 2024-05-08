#!/usr/bin/env python
# coding: utf-8

"""

An example task
===============

Goals:

* show basic task elements: requires, run, output
* command line integration
* client-server model

----

Simple command line integration
-------------------------------

    $ python x00.py
    No task specified

Expects a task names are argument
---------------------------------

    $ python x00.py ABC
    Traceback (most recent call last):
    ...
    luigi.task_register.TaskClassNotFoundException: No task ABC. Candidates are:
    Config,ExternalTask,MyTask,RangeBase,...

Client-Server
-------------

    $ python x00.py MyTask
    ...
    [2017-05-22 14:44:00][luigi-interface][ERROR   ]
        Failed connecting to remote scheduler 'http://localhost:8082'
    Traceback (most recent call last):
        ...
        raise ConnectionError(e, request=request)
    ConnectionError: HTTPConnectionPool(host='localhost', port=8082):
    Max retries exceeded with url: /api/add_task (
        Caused by NewConnectionError('<requests.packages. ...
        Failed to establish a new connection: [Errno 61] Connection refused',))

Finally
-------

    $ python x00.py MyTask --local-scheduler
    [2017-05-22 14:44:53][luigi-interface][INFO    ] Informed scheduler that task   MyTask__9...
    [2017-05-22 14:44:53][luigi-interface][INFO    ] Done scheduling tasks
    [2017-05-22 14:44:53][luigi-interface][INFO    ] Running Worker with 1 processes
    [2017-05-22 14:44:53][luigi.scheduler][INFO    ] Starting pruning of task graph
    [2017-05-22 14:44:53][luigi.scheduler][INFO    ] Done pruning task graph
    [2017-05-22 14:44:53][luigi-interface][INFO    ] [pid 40743] ... Worker(salt=8 ... running   MyTask()
    [2017-05-22 14:44:53][luigi-interface][INFO    ] [pid 40743] ... Worker(salt=8 ... MyTask()
    [2017-05-22 14:44:53][luigi-interface][INFO    ] Informed ...   MyTask__99914b932b   has status   DONE
    [2017-05-22 14:44:53][luigi.scheduler][INFO    ] Starting pruning of task graph
    [2017-05-22 14:44:53][luigi.scheduler][INFO    ] Done pruning task graph
    [2017-05-22 14:44:53][luigi-interface][INFO    ] Worker Worker(salt=889365910, ...
    [2017-05-22 14:44:53][luigi-interface][INFO    ]
    ===== Luigi Execution Summary =====

    Scheduled 1 tasks of which:
    * 1 ran successfully:
        - 1 MyTask()

    This progress looks :) because there were no failed tasks or missing external dependencies

    ===== Luigi Execution Summary =====

    $ cat x00.output.txt
    Some bytes written to a file.

"""

import luigi


class MyTask(luigi.Task):
    """
    A task encapsulates a processing step. It has requirements, business logic
    and some result.
    """

    def run(self):
        """
        Run just writes a string to the output file.
        """
        with self.output().open("w") as output:
            output.write("Some bytes written to a file.\n")

    def output(self):
        """
        Output target. Targets can be other things than files, see:
        https://git.io/vH0qI, e.g. MongoDB, MySQL, S3, Elasticsearch, ...
        """
        return luigi.LocalTarget(path="outputs/x00.txt")


if __name__ == "__main__":
    luigi.run(local_scheduler=True)
