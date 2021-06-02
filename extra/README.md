# Legacy LFTP

CentOS7 (centos7.9.2009) has an lftp version, that seems to work with certain
ftp endpoints, where newer versions fail (e.g. ftp.ieee.org).

You can create an image `lftplegacy` with:

```
$ make image
```

Drop a script in your path and let docker run your legacy lftp:

```
#!/bin/bash

# Run a legacy lftp for legacy FTP endpoints.

docker run -ti --rm lftplegacy "$@"
```
