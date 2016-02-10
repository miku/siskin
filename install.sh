#!/bin/sh
#
# Siskin install script. Also good for updates.
#

set -eo pipefail

echo "Installing siskin..."

PYVER=$(python -c 'import sys; print(".".join(map(str, sys.version_info[0:2])))')

if [[ "$PYVER" != "2.7" ]]; then
    echo "Python 2.7 required" && exit 1
fi

hash pip 2> /dev/null || { echo >&2 "pip is required."; exit 1; }

pip install -U siskin

echo "Installing command line tools..."

if [[ "$OSTYPE" == "linux-gnu" ]]; then
    if [ -f /etc/debian_version ]; then
        echo "Usings debs."
    elif [ -f /etc/redhat-release ]; then
        echo "Usings rpms."

        yum install -y jq xmlstarlet lftp vim tmux bash-completion tree
        yum install -y https://github.com/miku/esbulk/releases/download/v0.3.5/esbulk-0.3.5-0.x86_64.rpm
        yum install -y https://github.com/miku/filterline/releases/download/v0.1.3/filterline-0.1.3-0.x86_64.rpm
        yum install -y https://github.com/miku/hurrly/releases/download/v0.1.4/hurrly-0.1.4-0.x86_64.rpm
        yum install -y https://github.com/miku/memcldj/releases/download/v1.3.0/memcldj-1.3.0-0.x86_64.rpm
        yum install -y https://github.com/miku/solrbulk/releases/download/v0.1.5.4/solrbulk-0.1.5.4-0.x86_64.rpm
        yum install -y https://github.com/miku/span/releases/download/v0.1.59/span-0.1.59-0.x86_64.rpm
    else
        echo "Using binaries."
    fi
elif [[ "$OSTYPE" == "darwin"* ]]; then
    echo "[OS X] Using binaries."
elif [[ "$OSTYPE" == "cygwin" ]]; then
    echo "Not supported." && exit 1
elif [[ "$OSTYPE" == "msys" ]]; then
    echo "Not supported." && exit 1
elif [[ "$OSTYPE" == "win32" ]]; then
    echo "Not supported." && exit 1
elif [[ "$OSTYPE" == "freebsd"* ]]; then
    echo "Using binaries."
else
    exit 1
fi
