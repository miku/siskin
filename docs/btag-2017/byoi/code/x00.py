#!/usr/bin/env python
# coding: utf-8

"""
An example task.

Goal:

* Basic task elements (requires, run, output)
* Command line integration.
* Client server model.

Simple command line integration.

    $ python x00.py
    No task specified

Expects a task names are argument.

    $ python x00.py ABC
    Traceback (most recent call last):
    File "x00.py", line 32, in <module>
        luigi.run()
        ...
        raise TaskClassNotFoundException(cls._missing_task_msg(name))
    luigi.task_register.TaskClassNotFoundException: No task ABC. Candidates are:
    Config,ExternalTask,MyTask,RangeBase,RangeByMinutes,RangeByMinutesBase,RangeDaily,
    RangeDailyBase,RangeHourly,RangeHourlyBase,Task,TestNotificationsTask,
    WrapperTask,batch_email,core,email,execution_summary,scheduler,sendgrid,smtp,worker

Luigi has a client-server architecture.

    $ python x00.py MyTask
    [2017-05-22 14:44:00][requests.packages.urllib3.connectionpool][DEBUG   ] Starting new HTTP connection (1): localhost
    [2017-05-22 14:44:00][luigi-interface][ERROR   ] Failed connecting to remote scheduler 'http://localhost:8082'
    Traceback (most recent call last):
        ...
        raise ConnectionError(e, request=request)
    ConnectionError: HTTPConnectionPool(host='localhost', port=8082):
    Max retries exceeded with url: /api/add_task (
        Caused by NewConnectionError('<requests.packages.urllib3.connection.HTTPConnection object at 0x10a9bdad0>:
        Failed to establish a new connection: [Errno 61] Connection refused',))

Finally, it works:

    $ python x00.py MyTask --local-scheduler
    [2017-05-22 14:44:53][luigi-interface][INFO    ] Informed scheduler that task   MyTask__99914b932b   has status   PENDING
    [2017-05-22 14:44:53][luigi-interface][INFO    ] Done scheduling tasks
    [2017-05-22 14:44:53][luigi-interface][INFO    ] Running Worker with 1 processes
    [2017-05-22 14:44:53][luigi.scheduler][INFO    ] Starting pruning of task graph
    [2017-05-22 14:44:53][luigi.scheduler][INFO    ] Done pruning task graph
    [2017-05-22 14:44:53][luigi-interface][INFO    ] [pid 40743] Worker Worker(salt=889365910, workers=1, host=nexus, username=tir, pid=40743) running   MyTask()
    [2017-05-22 14:44:53][luigi-interface][INFO    ] [pid 40743] Worker Worker(salt=889365910, workers=1, host=nexus, username=tir, pid=40743) done      MyTask()
    [2017-05-22 14:44:53][luigi-interface][INFO    ] Informed scheduler that task   MyTask__99914b932b   has status   DONE
    [2017-05-22 14:44:53][luigi.scheduler][INFO    ] Starting pruning of task graph
    [2017-05-22 14:44:53][luigi.scheduler][INFO    ] Done pruning task graph
    [2017-05-22 14:44:53][luigi-interface][INFO    ] Worker Worker(salt=889365910, workers=1, host=nexus, username=tir, pid=40743) was stopped. Shutting down Keep-Alive thread
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

    def requires(self):
        pass

    def run(self):
        with self.output().open('w') as output:
            output.write('Some bytes written to a file.\n')

    def output(self):
        return luigi.LocalTarget(path='x00.output.txt')

if __name__ == '__main__':
    luigi.run()
