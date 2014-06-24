Troubleshooting
---------------

> I am getting OSError: [Errno 18] Invalid cross-device link?

The temporary directory (core.tempdir) and the home dir (core.home) must
reside on the save device. This is for atomicity.

> I am getting OSError: [Errno 2] No such file or directory: '.../tmp/gluish-WUEPIK'

Make sure the temporary directory you specified in core.tempdir exists.
It won't get created automatically (yet).
