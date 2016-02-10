#!/bin/bash
#
# Siskin install script. Also good for updates.
#
# TODO:
#
# - EPEL is needed on CentOS for xmlstarlet, jq, and potentially more.
# - github API limit while running ./install.sh over and over, script must stop here
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

echo "Installing command line tools..."

hash curl 2> /dev/null || { echo >&2 "curl is required."; exit 1; }

# install_latest_deb installs latest deb, given a username/repository on github.com.
install_latest_deb() {
    if [ $# -eq 0 ]; then
        echo "latest_deb_url expects an argument"
        exit 1
    fi
    URL=$(curl -s https://api.github.com/repos/$1/releases | jq '.[0].assets_url' | xargs curl -s | jq -r '.[].browser_download_url' | grep "deb")
    curl -o- "$URL" | dpkg --install
}

# install_latest_rpm rinstalls latest rpm, given a username/repository on github.com.
install_latest_rpm() {
    if [ $# -eq 0 ]; then
        echo "latest_rpm_url expects an argument"
        exit 1
    fi
    URL=$(curl -s https://api.github.com/repos/$1/releases | jq '.[0].assets_url' | xargs curl -s | jq -r '.[].browser_download_url' | grep "rpm")
    yum install -y "$URL"
}

if [[ "$OSTYPE" == "linux-gnu" ]]; then
    if [ -f /etc/debian_version ]; then
        echo "Usings debs."

        install_latest_deb "miku/span"
        install_latest_deb "miku/solrbulk"
        install_latest_deb "miku/memcldj"
        install_latest_deb "miku/hurrly"
        install_latest_deb "miku/esbulk"
        install_latest_deb "miku/oaimi"

    elif [ -f /etc/redhat-release ]; then
        echo "Usings rpms."

        yum -y install epel-release
        yum update
        yum groupinstall -y 'development tools'
        yum install -y jq xmlstarlet lftp vim tmux bash-completion tree libxml2 libxml2-devel python-devel libxslt-devel sqlite-devel

        install_latest_rpm "miku/span"
        install_latest_rpm "miku/solrbulk"
        install_latest_rpm "miku/memcldj"
        install_latest_rpm "miku/hurrly"
        install_latest_rpm "miku/esbulk"
        install_latest_rpm "miku/oaimi"
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
    echo "Unsupported OS."
    exit 1
fi

echo "Installing siskin..."

yum install -y python-pip

hash pip 2> /dev/null || { echo >&2 "pip is required. On Centos, python-pip is in EPEL."; exit 1; }

pip install -U siskin

cat <<EOF
  \. _(9>
    \==_)
     -'=
EOF
