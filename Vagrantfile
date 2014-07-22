# -*- mode: ruby -*-
# vi: set ft=ruby :

$script = <<SETUP_SCRIPT

# Adjust versions here ...
ELASTICSEARCH_VERSION=1.2.2
MARCTOOLS_VERSION=1.4.1

# Download Elasticsearch and install
ES_RPM=elasticsearch-$ELASTICSEARCH_VERSION.noarch.rpm
cd /tmp && wget -N "https://download.elasticsearch.org/elasticsearch/elasticsearch/$ES_RPM"
cd /tmp && yum install -y $ES_RPM

# Marc converters
cd /tmp && wget -O marctools-$MARCTOOLS_VERSION-0.x86_64.rpm "https://github.com/ubleipzig/marctools/releases/download/v$MARCTOOLS_VERSION/marctools-$MARCTOOLS_VERSION-0.x86_64.rpm"
cd /tmp && yum install -y marctools-$MARCTOOLS_VERSION-0.x86_64.rpm

# Enable EPEL
rpm -ivh http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm

# Install essential packages
yum groupinstall -y "Development tools"
yum install -y bash-completion
yum install -y blas-devel
yum install -y bzip2
yum install -y curl
yum install -y freetype
yum install -y freetype-devel
yum install -y git
yum install -y gzip
yum install -y java-1.7.0-openjdk
yum install -y lapack-devel
yum install -y lftp
yum install -y libpng-devel
yum install -y libxml2-devel
yum install -y libxslt
yum install -y libxslt-devel
yum install -y mysql-devel
yum install -y perl-XML-Twig
yum install -y php
yum install -y python-devel
yum install -y python-pip
yum install -y rpm-build
yum install -y rsync
yum install -y ruby-devel
yum install -y tar
yum install -y tree
yum install -y unzip
yum install -y wget
yum install -y yaz

# The packaging infrastructure (https://github.com/jordansissel/fpm)
gem install fpm

# upgrade setuptools
pip install -U setuptools

SETUP_SCRIPT

VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box = "centos65min"
  config.vm.provision "shell", inline: $script
end
