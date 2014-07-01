Troubleshooting
---------------

> I am getting OSError: [Errno 18] Invalid cross-device link?

The temporary directory `core.tempdir` and the home dir `core.home` must
reside on the save device. This is for atomicity.

----

> I am getting OSError: [Errno 24] Too many open files

A few workflows will touch hundreds or thousands of files. Systems
come with some limits, that might need to be extended.

By default, the maximum number of files that Mac OS X can open is set to
12,288 and the maximum number of files a given process can open is 10,240.
Relevant parameters:

    $ sysctl kern.maxfiles
    $ sysctl kern.maxfilesperproc

Adjust `/etc/sysctl.conf` for permanent changes. Also, the shell has its
own parameters, so you might want to extend those as well:

    $ ulimit -S -n 10240

See also:

* [Is there a fix for the “Too many open files in system” error on OS X 10.7.1?](http://superuser.com/a/443168/38447)
* [How do I change the number of open files limit in Linux?](http://stackoverflow.com/q/34588/89391)
