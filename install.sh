#!/bin/bash
#
# Siskin install script. Also good for updates.
#
# Notes:
#
# 1. EPEL is added.
#
# 2. We use the Github API to find latest versions of packages.
#    If you hit the API limit (normally you should not), sit back and wait.
#
# No VM at hand? Enjoy a installation screencast: https://asciinema.org/a/bl0r257503bnhxrydz7ucm6if

set -o pipefail

if [ "$EUID" -ne 0 ]
    then echo "superuser only"
    exit 1
fi

# install curl and wget first
if [[ "$OSTYPE" == "linux-gnu" ]]; then
    if [ -f /etc/debian_version ]; then
        apt-get update -y
        apt-get install -y wget curl
    elif [ -f /etc/redhat-release ]; then
        yum install -y wget curl
    else
        echo "not supported: $OSTYPE" && exit 1
    fi
else
    echo "not supported: $OSTYPE" && exit 1
fi

# we don't stop on yum failure (yum seems to return 1 if everything is up to date), so check here explictly
hash curl 2> /dev/null || { echo >&2 "curl is required."; exit 1; }
hash wget 2> /dev/null || { echo >&2 "wget is required."; exit 1; }

# install_latest_deb installs latest deb release, given a username/repository on github.com.
install_latest_deb() {
    if [ $# -eq 0 ]; then
        echo "install_latest_deb expects a username/repository as argument"
        exit 1
    fi

    # if we failed to install jq, complain here
    hash jq 2> /dev/null || { echo >&2 "jq is required."; exit 1; }

    URL=$(curl -s https://api.github.com/repos/$1/releases | jq '.[0].assets_url' | xargs curl -s | jq -r '.[].browser_download_url' | grep "deb")
    RC=$?; if [[ $RC != 0 ]]; then
        echo "cannot find latest package for $1, maybe hit API limits?"
        exit $RC;
    fi

    # dpkg cannot handle urls?
    mkdir -p $TMPDIR
    wget -O $TMPDIR/transit.deb "$URL" && dpkg -i $TMPDIR/transit.deb && rm -f $TMPDIR/transit.deb
}

# install_latest_rpm installs latest rpm release, given a username/repository on github.com.
install_latest_rpm() {
    if [ $# -eq 0 ]; then
        echo "install_latest_rpm expects a username/repository as argument"
        exit 1
    fi

    # if for some reason we failed to install jq bail out
    hash jq 2> /dev/null || { echo >&2 "jq is required."; exit 1; }

    URL=$(curl -s https://api.github.com/repos/$1/releases | jq '.[0].assets_url' | xargs curl -s | jq -r '.[].browser_download_url' | grep "rpm")
    RC=$?; if [[ $RC != 0 ]]; then
        echo "cannot find latest package for $1, maybe hit API limits?"
        exit $RC;
    fi
    yum install -y "$URL"
}

# centos_6_install_python_27 install Python 2.7 safely with altinstall.
centos_6_install_python_27() {
    yum install -y zlib-dev openssl-devel sqlite-devel bzip2-devel xz-libs

    cd $TMPDIR
    wget -O Python-2.7.11.tar.xz https://www.python.org/ftp/python/2.7.11/Python-2.7.11.tar.xz
    tar xf Python-2.7.11.tar.xz
    cd Python-2.7.11
    ./configure --prefix=/usr/local && make && make altinstall
    cd /usr/local/bin
    ln -s python2.7 python
    ln -s python2.7-config python-config
    cd $TMPDIR

    wget https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py
    /usr/local/bin/python2.7 ez_setup.py
    /usr/local/bin/easy_install-2.7 pip

    [ ! -e "/etc/profile.d/extrapath.sh" ] && sh -c 'echo "export PATH=\"/usr/local/bin:/usr/local/sbin:$PATH\"" > /etc/profile.d/extrapath.sh'
    source /etc/profile.d/extrapath.sh
}

if [[ "$OSTYPE" == "linux-gnu" ]]; then
    if [ -f /etc/debian_version ]; then

        apt-get install -y build-essential gcc make autoconf flex bison binutils
        apt-get install -y pigz jq xmlstarlet lftp vim tmux bash-completion tree libxml2 libxml2-dev python-dev libxslt1-dev libsqlite3-dev

        install_latest_deb "miku/span"
        install_latest_deb "miku/solrbulk"
        install_latest_deb "miku/memcldj"
        install_latest_deb "miku/hurrly"
        install_latest_deb "miku/esbulk"
        install_latest_deb "miku/oaimi"

    elif [ -f /etc/redhat-release ]; then
        # extract CentOS version
        VERSION=$(rpm -q --queryformat '%{VERSION}' centos-release)

        if [[ ! ("$VERSION" == "6" || "$VERSION" == "7") ]]; then
            echo "only CentOS 6 and 7 support implemented" && exit 1
        fi

        if [[ "$VERSION" == "6" ]]; then
            PYVERSION=$(python -c 'import sys; print("%s.%s" % (sys.version_info[0], sys.version_info[1]))')
            if [ $PYVERSION != "2.7" ]; then
                centos_6_install_python_27
            fi
            rpm -ivh http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
            yum update -y
        fi

        if [[ "$VERSION" == "7" ]]; then
            yum install -y epel-release
            yum update -y
        fi

        yum groupinstall -y 'development tools'
        yum install -y pigz jq xmlstarlet lftp vim tmux bash-completion tree libxml2 libxml2-devel python-devel libxslt-devel sqlite-devel

        install_latest_rpm "miku/span"
        install_latest_rpm "miku/solrbulk"
        install_latest_rpm "miku/memcldj"
        install_latest_rpm "miku/hurrly"
        install_latest_rpm "miku/esbulk"
        install_latest_rpm "miku/oaimi"
    else
        echo "not supported: $OSTYPE" && exit 1
    fi
else
    echo "not supported: $OSTYPE" && exit 1
fi

# install pip
if [[ "$OSTYPE" == "linux-gnu" ]]; then
    if [ -f /etc/debian_version ]; then
        apt-get install -y python-pip
    elif [ -f /etc/redhat-release ]; then
        yum install -y python-pip
    else
        echo "not supported: $OSTYPE" && exit 1
    fi
else
    echo "not supported: $OSTYPE" && exit 1
fi

# ensure pip is installed
hash pip 2> /dev/null || { echo >&2 "pip is required. On Centos, python-pip is in EPEL."; exit 1; }

pip install --upgrade pip
pip install --upgrade siskin

# Download file from master branch: https://raw.githubusercontent.com/miku/siskin/master/<PATH>
# Existing files are not overwritten.
#
#   $ tree etc
#   etc
#   ├── bash_completion.d
#   │   └── siskin_completion.sh
#   ├── luigi
#   │   ├── client.cfg
#   │   └── logging.ini
#   └── siskin
#       └── siskin.ini
#
download_file_from_github() {
    BASE=https://raw.githubusercontent.com/miku/siskin/master
    if [ "$#" -eq 0 ]; then
        echo "download_config_file_from_github expects a path as argument, which is appended to $BASE"
        exit 1
    fi
    mkdir -p $(basename "$1") && [ ! -f "$1" ] && wget -O "$1" "$BASE$1"
}

download_file_from_github "/etc/luigi/client.cfg"
download_file_from_github "/etc/luigi/logging.ini"
download_file_from_github "/etc/siskin/siskin.ini"
download_file_from_github "/etc/bash_completion.d/siskin_completion.sh"

chmod +x /etc/bash_completion.d/siskin_completion.sh
