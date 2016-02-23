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

set -o pipefail

if [ "$EUID" -ne 0 ]
    then echo "superuser only"
    exit 1
fi

echo "installing command line tools..."

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

# we don't stop on yum failure, so check explictly
hash curl 2> /dev/null || { echo >&2 "curl is required."; exit 1; }
hash wget 2> /dev/null || { echo >&2 "wget is required."; exit 1; }

# install_latest_deb installs latest deb, given a username/repository on github.com.
install_latest_deb() {
    if [ $# -eq 0 ]; then
        echo "latest_deb_url expects an argument"
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

# install_latest_rpm rinstalls latest rpm, given a username/repository on github.com.
install_latest_rpm() {
    if [ $# -eq 0 ]; then
        echo "latest_rpm_url expects an argument"
        exit 1
    fi

    # if we failed to install jq, complain here
    hash jq 2> /dev/null || { echo >&2 "jq is required."; exit 1; }

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
        apt-get install -y jq xmlstarlet lftp vim tmux bash-completion tree libxml2 libxml2-dev python-dev libxslt1-dev libsqlite3-dev

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
        echo "not supported: $OSTYPE" && exit 1
    fi
else
    echo "not supported: $OSTYPE" && exit 1
fi

echo "installing siskin..."

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
