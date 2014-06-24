Troubleshooting
---------------

> I am getting OSError: [Errno 18] Invalid cross-device link?

The temporary directory (core.tempdir) and the home dir (core.home) must
reside on the save device. This is for atomicity.
