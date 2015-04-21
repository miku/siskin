# -*- mode: ruby -*-
# vi: set ft=ruby :

$script = <<SETUP_SCRIPT

# Enable EPEL
rpm -ivh http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm

# Install essential packages
yum groupinstall -y "Development tools"
yum install -y bash-completion
yum install -y bzip2
yum install -y curl
yum install -y git
yum install -y gzip
yum install -y lftp
yum install -y libpng-devel
yum install -y libxml2-devel
yum install -y libxslt
yum install -y libxslt-devel
yum install -y perl-XML-Twig
yum install -y php
yum install -y python-devel
yum install -y python-pip
yum install -y rpm-build
yum install -y rsync
yum install -y sqlite-devel
yum install -y tar
yum install -y tree
yum install -y unzip
yum install -y vim
yum install -y wget
yum install -y yaz

yum install -y python-virtualenvwrapper

mkdir -p /home/vagrant/code/miku
cd /home/vagrant/code/miku && git clone https://github.com/miku/siskin.git


SETUP_SCRIPT

VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box = "chef/centos-6.5"
  config.vm.provision "shell", inline: $script
end
