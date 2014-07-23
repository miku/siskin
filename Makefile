help:
	@echo "make clean"

clean:
	rm -rf siskin.egg-info
	rm -rf build/ dist/
	rm -rf python-*.rpm

# packaging via vagrant
SSHCMD = ssh -o StrictHostKeyChecking=no -i vagrant.key vagrant@127.0.0.1 -p 2222

# Helper to build RPM on a RHEL6 VM, to link against glibc 2.12
vagrant.key:
	curl -sL "https://raw.githubusercontent.com/mitchellh/vagrant/master/keys/vagrant" > vagrant.key
	chmod 0600 vagrant.key

setup: vagrant.key
	$(SSHCMD) git clone https://github.com/miku/siskin.git

# this will take a few minutes
republish: all createrepo
	@echo "Now: yum clean all && yum update"

# this will take a few seconds (and works fine if deps didn't change)
publish: package createrepo
	@echo "Now: yum clean all && yum update"

# make sure /usr/share/nginx/html/repo/CentOS/6/x86_64 exists and is writable
createrepo:
	rm /usr/share/nginx/html/repo/CentOS/6/x86_64/*rpm
	rm -rf /usr/share/nginx/html/repo/CentOS/6/x86_64/repodata
	cp dist/python-*.rpm /usr/share/nginx/html/repo/CentOS/6/x86_64
	createrepo /usr/share/nginx/html/repo/CentOS/6/x86_64

all: vagrant.key
	$(SSHCMD) "cd siskin && make vm-all"

package: vagrant.key
	$(SSHCMD) "cd siskin && make vm-package"

/vargant/dist:
	mkdir -p /vagrant/dist

vm-all: clean /vargant/dist
	git pull origin master
	cat requirements.txt | while read line; do fpm --force --verbose -s python -t rpm $$line; done
	fpm --force --verbose -s python -t rpm .
	cp python-*.rpm /vagrant/dist

vm-package: clean /vargant/dist
	git pull origin master
	fpm --force --verbose -s python -t rpm .
	cp python-*.rpm /vagrant/dist
