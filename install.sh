#!/bin/sh
#
# Siskin install script. Also good for updates.
#

set -eo pipefail

echo "Installing siskin..."

if [[ "$OSTYPE" == "linux-gnu" ]]; then
    if [ -f /etc/debian_version ]; then
        echo "Usings debs."
    elif [ -f /etc/redhat-release ]; then
        echo "Usings rpms."
    else
        echo "Using binaries."
    fi
elif [[ "$OSTYPE" == "darwin"* ]]; then
    echo "[OS X] Using binaries."
elif [[ "$OSTYPE" == "cygwin" ]]; then
    echo "Not supported."
elif [[ "$OSTYPE" == "msys" ]]; then
    echo "Not supported."
elif [[ "$OSTYPE" == "win32" ]]; then
    echo "Not supported."
elif [[ "$OSTYPE" == "freebsd"* ]]; then
    echo "Using binaries."
else
    exit 1
fi
