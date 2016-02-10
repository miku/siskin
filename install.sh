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

# centos_6_install_python_27 install Pytohn 2.7 safely with altinstall.
centos_6_install_python_27() {
    yum install -y zlib-dev openssl-devel sqlite-devel bzip2-devel xz-libs
    cd /tmp
    wget https://www.python.org/ftp/python/2.7.11/Python-2.7.11.tar.xz
    tar xf Python-2.7.11.tar.xz
    cd Python-2.7.11
    ./configure --prefix=/usr/local && make && make altinstall
    cd /usr/local/bin
    ln -s python2.7 python
    ln -s python2.7-config python-config
    cd /tmp
    wget https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py
    /usr/local/bin/python2.7 ez_setup.py
    /usr/local/bin/easy_install-2.7 pip
    [ ! -e "/etc/profile.d/extrapath.sh" ] && sh -c 'echo "export PATH=\"/usr/local/bin:/usr/local/sbin:$PATH\"" > /etc/profile.d/extrapath.sh'
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

        if [[ ! ("$VERSION" == "6" || "$VERSION" == "7") ]]; then
            echo "only CentOS 6 and 7 support implemented" && exit 1
        fi

        if [[ "$VERSION" == "6" ]]; then
            centos_6_install_python_27
            rpm -ivh http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
            yum update -y
        fi

        if [[ "$VERSION" == "7" ]]; then
            yum install -y epel-release
            yum update -y
        fi

        yum groupinstall -y 'development tools'
        yum install -y jq xmlstarlet lftp vim tmux bash-completion tree libxml2 libxml2-devel python-devel libxslt-devel sqlite-devel

        install_latest_rpm "miku/span"
        install_latest_rpm "miku/solrbulk"
        install_latest_rpm "miku/memcldj"
        install_latest_rpm "miku/hurrly"
        install_latest_rpm "miku/esbulk"
        install_latest_rpm "miku/oaimi"
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
