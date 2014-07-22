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

vm-setup: vagrant.key
	$(SSHCMD) git clone https://github.com/miku/siskin.git

# make sure /usr/share/nginx/html/repo/CentOS/6/x86_64 exists and is writable
createrepo:
	cp dist/python*.rpm /usr/share/nginx/html/repo/CentOS/6/x86_64
	createrepo /usr/share/nginx/html/repo/CentOS/6/x86_64

all-packages:
	git pull origin master
	cat requirements.txt | while read line; do fpm --verbose -s python -t rpm $$line; done
	fpm -s python -t rpm .
	cp python*rpm /vagrant/dist

package:
	fpm --force -s python -t rpm .
	cp python*rpm /vagrant/dist
