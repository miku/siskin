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
# TODO: CentOS 6 (must add Python 2.7), Ubuntu, Mac OS X (must be precompiled)
#

set -o pipefail

if [ "$EUID" -ne 0 ]
  then echo "Please run as root"
  exit
fi

PYVER=$(python -c 'import sys; print(".".join(map(str, sys.version_info[0:2])))')

if [[ "$PYVER" != "2.7" ]]; then
    echo "Python 2.7 required" && exit 1
fi

echo "installing command line tools..."

yum install -y wget curl

# we don't stop on yum failure, so check explictly
hash curl 2> /dev/null || { echo >&2 "curl is required."; exit 1; }
hash wget 2> /dev/null || { echo >&2 "wget is required."; exit 1; }

# install_latest_deb installs latest deb, given a username/repository on github.com.
install_latest_deb() {
    if [ $# -eq 0 ]; then
        echo "latest_deb_url expects an argument"
        exit 1
    fi
    URL=$(curl -s https://api.github.com/repos/$1/releases | jq '.[0].assets_url' | xargs curl -s | jq -r '.[].browser_download_url' | grep "deb")
    RC=$?; if [[ $RC != 0 ]]; then
        echo "cannot find latest package for $1, maybe hit API limits?"
        exit $RC;
    fi
    curl -o- "$URL" | dpkg --install
}

# install_latest_rpm rinstalls latest rpm, given a username/repository on github.com.
install_latest_rpm() {
    if [ $# -eq 0 ]; then
        echo "latest_rpm_url expects an argument"
        exit 1
    fi
    URL=$(curl -s https://api.github.com/repos/$1/releases | jq '.[0].assets_url' | xargs curl -s | jq -r '.[].browser_download_url' | grep "rpm")
    RC=$?; if [[ $RC != 0 ]]; then
        echo "cannot find latest package for $1, maybe hit API limits?"
        exit $RC;
    fi
    yum install -y "$URL"
}

if [[ "$OSTYPE" == "linux-gnu" ]]; then
    if [ -f /etc/debian_version ]; then

        install_latest_deb "miku/span"
        install_latest_deb "miku/solrbulk"
        install_latest_deb "miku/memcldj"
        install_latest_deb "miku/hurrly"
        install_latest_deb "miku/esbulk"
        install_latest_deb "miku/oaimi"

    elif [ -f /etc/redhat-release ]; then

        VERSION=$(rpm -q --queryformat '%{VERSION}' centos-release)

        if [[ "$VERSION" == "7" ]]; then
            yum install -y epel-release
            yum update -y
            yum groupinstall -y 'development tools'
            yum install -y jq xmlstarlet lftp vim tmux bash-completion tree libxml2 libxml2-devel python-devel libxslt-devel sqlite-devel

            install_latest_rpm "miku/span"
            install_latest_rpm "miku/solrbulk"
            install_latest_rpm "miku/memcldj"
            install_latest_rpm "miku/hurrly"
            install_latest_rpm "miku/esbulk"
            install_latest_rpm "miku/oaimi"
        else
            echo "not yet supported: $VERSION"
            exit 1
        fi
    else
        echo "TODO: [linux] using binaries... " && exit 1
    fi
elif [[ "$OSTYPE" == "darwin"* ]]; then
    echo "TODO: [osx] using binaries... " && exit 1
else
    echo "not supported: %OSTYPE" && exit 1
fi

echo "installing siskin..."

yum install -y python-pip

hash pip 2> /dev/null || { echo >&2 "pip is required. On Centos, python-pip is in EPEL."; exit 1; }

pip install -U siskin

echo "setting up configuration..."

mkdir /etc/siskin
mkdir /etc/luigi

wget -O /etc/luigi/client.cfg https://raw.githubusercontent.com/miku/siskin/master/etc/luigi/client.cfg
wget -O /etc/luigi/logging.ini https://raw.githubusercontent.com/miku/siskin/master/etc/luigi/logging.ini
wget -O /etc/siskin/siskin.ini https://raw.githubusercontent.com/miku/siskin/master/etc/siskin/siskin.example.ini

echo "setting up autocomplete..."

sudo mkdir -p /etc/bash_completion.d/
sudo wget -O /etc/bash_completion.d/siskin_completion.sh https://raw.githubusercontent.com/miku/siskin/master/contrib/siskin_completion.sh
sudo chmod +x /etc/bash_completion.d/siskin_completion.sh

cat <<EOF
  \. _(9>
    \==_)
     -'=
EOF
