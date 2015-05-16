SHELL := /bin/bash

help:
	@echo "make clean"

clean:
	rm -rf siskin.egg-info
	rm -rf build/ dist/
	rm -rf python-*.rpm
	rm -f siskin.pex

# packaging via vagrant
PORT = 2222
SSHCMD = ssh -o StrictHostKeyChecking=no -i vagrant.key vagrant@127.0.0.1 -p $(PORT)
REPOPATH = /usr/share/nginx/html/repo/CentOS/6/x86_64
PROJECTGIT = https://github.com/miku/siskin.git

# helper targets to build RPM on a RHEL6 VM, to link against glibc 2.12
vagrant.key:
	curl -sL "https://raw.githubusercontent.com/mitchellh/vagrant/master/keys/vagrant" > vagrant.key
	chmod 0600 vagrant.key

# run this after first 'vagrant up'
setup: vagrant.key
	$(SSHCMD) git clone $(PROJECTGIT)

# this will take a few minutes
republish: all createrepo
	@echo "Now: yum clean all && yum update"

# this will take a few seconds (and works fine if deps didn't change)
publish: package createrepo
	@echo "Now: yum clean all && yum update"

# make sure $(REPOPATH) exists and is writable
createrepo:
	rm $(REPOPATH)/*rpm
	rm -rf $(REPOPATH)/repodata
	cp dist/python-*.rpm $(REPOPATH)
	createrepo $(REPOPATH)

all: vagrant.key
	$(SSHCMD) "cd siskin && make vm-all"

package: vagrant.key
	$(SSHCMD) "cd siskin && make vm-package"

/vargant/dist:
	mkdir -p /vagrant/dist

# inside vm only:
vm-all: clean /vargant/dist
	git pull origin master
	cat requirements.txt | while read line; do fpm --force --verbose -s python -t rpm $$line; done
	fpm --force --verbose -s python -t rpm .
	cp python-*.rpm /vagrant/dist

vm-package: clean /vargant/dist
	git pull origin master
	fpm --force --verbose -s python -t rpm .
	cp python-*.rpm /vagrant/dist

# ----

siskin.pex:
	pex -r <(pip freeze|grep -v wsgiref) --python=python2.6 -o siskin.pex

